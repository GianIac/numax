use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::time::Duration;

use super::suspicion::{Health, Timing};
use super::{Error, Version, deadline};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Peer {
    pub(super) id: String,
    pub(super) version: Version,
    pub(super) session: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Outcome {
    Success,
    Failed,
    LocalDegradation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Action {
    Direct {
        attempt: u64,
        target: Peer,
        buddy: bool,
    },
    Indirect {
        attempt: u64,
        helper: Peer,
        target: Peer,
        deadline: Duration,
        buddy: bool,
    },
    Finished {
        attempt: u64,
        target: Peer,
        outcome: Outcome,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HelperState {
    Queued,
    Dispatched,
    Nack,
}

struct Attempt {
    target: Peer,
    helpers: BTreeMap<String, (Peer, HelperState)>,
    buddy: bool,
    direct_timeout: Duration,
    period: Duration,
    queue_deadline: Duration,
    direct_deadline: Option<Duration>,
    deadline: Option<Duration>,
    indirect_started: bool,
}

pub(super) struct Probes {
    timing: Timing,
    capacity: usize,
    helper_limit: usize,
    serial: u64,
    now: Duration,
    attempts: BTreeMap<u64, Attempt>,
}

impl Probes {
    pub(super) fn is_full(&self) -> bool {
        self.attempts.len() >= self.capacity
    }

    pub(super) fn helper_limit(&self) -> usize {
        self.helper_limit
    }

    pub(super) fn invalidate(&mut self, member: Option<&str>) -> Vec<Action> {
        let ids: Vec<_> = self
            .attempts
            .iter()
            .filter(|(_, attempt)| {
                member.is_none_or(|member| {
                    attempt.target.id == member || attempt.helpers.contains_key(member)
                })
            })
            .map(|(id, _)| *id)
            .collect();
        let mut actions = Vec::new();
        for attempt in ids {
            if let Some(current) = self.attempts.remove(&attempt) {
                actions.push(Action::Finished {
                    attempt,
                    target: current.target,
                    outcome: Outcome::LocalDegradation,
                });
            }
        }
        actions
    }

    pub(super) fn new(timing: Timing, capacity: usize, helper_limit: usize) -> Result<Self, Error> {
        if capacity == 0 || capacity > 64 || helper_limit > 16 {
            return Err(Error::InvalidConfig);
        }
        Ok(Self {
            timing: timing.validate()?,
            capacity,
            helper_limit,
            serial: 0,
            now: Duration::ZERO,
            attempts: BTreeMap::new(),
        })
    }

    fn advance(&mut self, now: Duration) -> Result<(), Error> {
        if now < self.now {
            return Err(Error::ClockRegression);
        }
        self.now = now;
        Ok(())
    }

    pub(super) fn start(
        &mut self,
        target: Peer,
        helpers: Vec<Peer>,
        buddy: bool,
        now: Duration,
        health: Health,
    ) -> Result<Vec<Action>, Error> {
        self.advance(now)?;
        if self.attempts.len() >= self.capacity || helpers.len() > self.helper_limit {
            return Err(Error::Capacity);
        }
        let valid = |peer: &Peer| {
            !peer.id.is_empty()
                && peer.id.len() <= 256
                && peer.session != 0
                && peer.version.epoch != 0
        };
        let mut helper_map = BTreeMap::new();
        if !valid(&target) {
            return Err(Error::Unauthorized);
        }
        for helper in helpers {
            if !valid(&helper) || helper.id == target.id || helper_map.contains_key(&helper.id) {
                return Err(Error::Unauthorized);
            }
            helper_map.insert(helper.id.clone(), (helper, HelperState::Queued));
        }
        let period = health.scale(self.timing.interval)?;
        let direct_timeout = health.scale(self.timing.direct)?;
        let queue_deadline = deadline(now, period)?;
        let attempt = self.serial.checked_add(1).ok_or(Error::Overflow)?;
        let action = Action::Direct {
            attempt,
            target: target.clone(),
            buddy,
        };
        self.attempts.insert(
            attempt,
            Attempt {
                target,
                helpers: helper_map,
                buddy,
                direct_timeout,
                period,
                queue_deadline,
                direct_deadline: None,
                deadline: None,
                indirect_started: false,
            },
        );
        self.serial = attempt;
        Ok(vec![action])
    }

    pub(super) fn dispatched(
        &mut self,
        attempt: u64,
        route: &Peer,
        now: Duration,
    ) -> Result<Vec<Action>, Error> {
        self.advance(now)?;
        let Some(current) = self.attempts.get(&attempt) else {
            return Ok(Vec::new());
        };
        if current.deadline.is_some_and(|end| now >= end)
            || (current.deadline.is_none() && now >= current.queue_deadline)
        {
            return self.finish(attempt, Outcome::LocalDegradation, None);
        }
        if route == &current.target {
            if current.deadline.is_some() {
                return Ok(Vec::new());
            }
            let direct = deadline(now, current.direct_timeout)?;
            let end = deadline(now, current.period)?;
            let current = self.attempts.get_mut(&attempt).ok_or(Error::NotReady)?;
            current.direct_deadline = Some(direct);
            current.deadline = Some(end);
        } else {
            let current = self.attempts.get_mut(&attempt).ok_or(Error::NotReady)?;
            if !current.indirect_started {
                return Ok(Vec::new());
            }
            if let Some((helper, state)) = current.helpers.get_mut(&route.id)
                && helper == route
                && *state == HelperState::Queued
            {
                *state = HelperState::Dispatched;
            }
        }
        Ok(Vec::new())
    }

    pub(super) fn reply(
        &mut self,
        attempt: u64,
        target: &Peer,
        route: &Peer,
        ack: bool,
        now: Duration,
        health: &mut Health,
    ) -> Result<Vec<Action>, Error> {
        self.advance(now)?;
        let Some(current) = self.attempts.get(&attempt) else {
            return Ok(Vec::new());
        };
        if current.target != *target || current.deadline.is_none() {
            return Ok(Vec::new());
        }
        if current.deadline.is_some_and(|end| now >= end) {
            return self.expire(attempt, health);
        }
        let direct = route == &current.target;
        let helper = current.helpers.get(&route.id).is_some_and(|(peer, state)| {
            peer == route && matches!(state, HelperState::Dispatched | HelperState::Nack)
        });
        if !(helper || direct && ack) {
            return Ok(Vec::new());
        }
        if ack {
            return self.finish(attempt, Outcome::Success, Some(health));
        }
        let current = self.attempts.get_mut(&attempt).ok_or(Error::NotReady)?;
        if let Some((_, state)) = current.helpers.get_mut(&route.id) {
            *state = HelperState::Nack;
        }
        Ok(Vec::new())
    }

    pub(super) fn cancel(&mut self, attempt: u64, now: Duration) -> Result<Vec<Action>, Error> {
        self.advance(now)?;
        self.finish(attempt, Outcome::LocalDegradation, None)
    }

    pub(super) fn tick(
        &mut self,
        now: Duration,
        health: &mut Health,
    ) -> Result<Vec<Action>, Error> {
        self.advance(now)?;
        let ids: Vec<_> = self.attempts.keys().copied().collect();
        let mut actions = Vec::new();
        for attempt in ids {
            let current = &self.attempts[&attempt];
            if current.deadline.is_none() {
                if now >= current.queue_deadline {
                    actions.extend(self.finish(attempt, Outcome::LocalDegradation, None)?);
                }
            } else if current.deadline.is_some_and(|end| now >= end) {
                actions.extend(self.expire(attempt, health)?);
            } else if !current.indirect_started
                && current.direct_deadline.is_some_and(|end| now >= end)
            {
                let current = self.attempts.get_mut(&attempt).ok_or(Error::NotReady)?;
                current.indirect_started = true;
                for (helper, _) in current.helpers.values() {
                    actions.push(Action::Indirect {
                        attempt,
                        helper: helper.clone(),
                        target: current.target.clone(),
                        deadline: current.deadline.ok_or(Error::NotReady)?,
                        buddy: current.buddy,
                    });
                }
            }
        }
        Ok(actions)
    }

    fn expire(&mut self, attempt: u64, health: &mut Health) -> Result<Vec<Action>, Error> {
        let Some(current) = self.attempts.get(&attempt) else {
            return Ok(Vec::new());
        };
        if !current.indirect_started && !current.helpers.is_empty()
            || current
                .helpers
                .values()
                .any(|(_, state)| *state == HelperState::Queued)
        {
            return self.finish(attempt, Outcome::LocalDegradation, None);
        }
        self.finish(attempt, Outcome::Failed, Some(health))
    }

    fn finish(
        &mut self,
        attempt: u64,
        outcome: Outcome,
        health: Option<&mut Health>,
    ) -> Result<Vec<Action>, Error> {
        let Some(current) = self.attempts.remove(&attempt) else {
            return Ok(Vec::new());
        };
        if let Some(health) = health {
            match outcome {
                Outcome::Success => health.success(),
                Outcome::Failed => health.failure(
                    current
                        .helpers
                        .values()
                        .any(|(_, state)| *state == HelperState::Dispatched),
                    self.timing.health_max,
                ),
                Outcome::LocalDegradation => {}
            }
        }
        Ok(vec![Action::Finished {
            attempt,
            target: current.target,
            outcome,
        }])
    }
}

pub(super) struct Schedule {
    local: String,
    capacity: usize,
    remaining: VecDeque<String>,
    visited: BTreeSet<String>,
}

impl Schedule {
    pub(super) fn new(local: String, capacity: usize) -> Result<Self, Error> {
        if local.is_empty() || local.len() > 256 || capacity == 0 || capacity > 4096 {
            return Err(Error::InvalidConfig);
        }
        Ok(Self {
            local,
            capacity,
            remaining: VecDeque::new(),
            visited: BTreeSet::new(),
        })
    }

    pub(super) fn next(
        &mut self,
        eligible: &[String],
        mut choose: impl FnMut(usize) -> usize,
    ) -> Result<Option<String>, Error> {
        if eligible.len() > self.capacity {
            return Err(Error::Capacity);
        }
        let members: BTreeSet<_> = eligible
            .iter()
            .filter(|member| **member != self.local)
            .cloned()
            .collect();
        if members
            .iter()
            .any(|member| member.is_empty() || member.len() > 256)
        {
            return Err(Error::Capacity);
        }
        let mut remaining: VecDeque<_> = self
            .remaining
            .iter()
            .filter(|member| members.contains(*member))
            .cloned()
            .collect();
        let mut visited: BTreeSet<_> = self.visited.intersection(&members).cloned().collect();
        if remaining.is_empty() && members.iter().all(|member| visited.contains(member)) {
            visited.clear();
        }
        let mut fresh: Vec<_> = members
            .iter()
            .filter(|member| !visited.contains(*member) && !remaining.contains(member))
            .cloned()
            .collect();
        for upper in (2..=fresh.len()).rev() {
            let index = choose(upper);
            if index >= upper {
                return Err(Error::InvalidConfig);
            }
            fresh.swap(upper - 1, index);
        }
        remaining.extend(fresh);
        let next = remaining.pop_front();
        if let Some(member) = &next {
            visited.insert(member.clone());
        }
        self.remaining = remaining;
        self.visited = visited;
        Ok(next)
    }
}

#[cfg(test)]
mod tests {
    use super::super::suspicion::tests::timing;
    use super::*;

    fn peer(id: &str, session: u64) -> Peer {
        Peer {
            id: id.into(),
            version: Version {
                epoch: 1,
                incarnation: 0,
            },
            session,
        }
    }

    fn begin(helpers: Vec<Peer>) -> (Probes, Health, u64, Peer) {
        let mut probes = Probes::new(timing(), 2, 3).unwrap();
        let health = Health::default();
        let target = peer("bob", 11);
        let actions = probes
            .start(target.clone(), helpers, true, Duration::ZERO, health)
            .unwrap();
        let Action::Direct { attempt, buddy, .. } = actions[0] else {
            panic!()
        };
        assert!(buddy);
        probes.dispatched(attempt, &target, Duration::ZERO).unwrap();
        (probes, health, attempt, target)
    }

    fn outcome(actions: Vec<Action>) -> Outcome {
        let Action::Finished { outcome, .. } = actions[0] else {
            panic!()
        };
        outcome
    }

    #[test]
    fn direct_success_is_correlated_and_duplicate_feedback_is_ignored() {
        let (mut probes, mut health, attempt, target) = begin(Vec::new());
        health.failure(false, 8);
        let mut stale = target.clone();
        stale.session += 1;
        assert!(
            probes
                .reply(attempt, &target, &stale, true, Duration::ZERO, &mut health)
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            outcome(
                probes
                    .reply(attempt, &target, &target, true, Duration::ZERO, &mut health)
                    .unwrap()
            ),
            Outcome::Success
        );
        assert_eq!(health.score(), 0);
        assert!(
            probes
                .reply(attempt, &target, &target, true, Duration::ZERO, &mut health)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn helper_nack_does_not_prevent_a_later_timely_ack() {
        let helper = peer("carol", 12);
        let (mut probes, mut health, attempt, target) = begin(vec![helper.clone()]);
        assert!(
            probes
                .reply(attempt, &target, &helper, true, Duration::ZERO, &mut health)
                .unwrap()
                .is_empty()
        );
        let actions = probes
            .tick(Duration::from_millis(200), &mut health)
            .unwrap();
        assert!(matches!(&actions[0], Action::Indirect { buddy: true, .. }));
        probes
            .dispatched(attempt, &helper, Duration::from_millis(200))
            .unwrap();
        assert!(
            probes
                .reply(
                    attempt,
                    &target,
                    &helper,
                    false,
                    Duration::from_millis(300),
                    &mut health
                )
                .unwrap()
                .is_empty()
        );
        assert!(
            probes
                .reply(
                    attempt,
                    &target,
                    &helper,
                    false,
                    Duration::from_millis(300),
                    &mut health
                )
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            outcome(
                probes
                    .reply(
                        attempt,
                        &target,
                        &helper,
                        true,
                        Duration::from_millis(400),
                        &mut health
                    )
                    .unwrap()
            ),
            Outcome::Success
        );
    }

    #[test]
    fn helpers_missing_nacks_and_no_helpers_have_bounded_feedback() {
        for helpers in [Vec::new(), vec![peer("carol", 12)]] {
            let expected = if helpers.is_empty() { 1 } else { 2 };
            let (mut probes, mut health, attempt, _) = begin(helpers.clone());
            probes
                .tick(Duration::from_millis(200), &mut health)
                .unwrap();
            for helper in helpers {
                probes
                    .dispatched(attempt, &helper, Duration::from_millis(200))
                    .unwrap();
            }
            assert_eq!(
                outcome(probes.tick(Duration::from_secs(1), &mut health).unwrap()),
                Outcome::Failed
            );
            assert_eq!(health.score(), expected);
            assert!(
                probes
                    .tick(Duration::from_secs(2), &mut health)
                    .unwrap()
                    .is_empty()
            );
            assert_eq!(health.score(), expected);
        }
    }

    #[test]
    fn all_nacks_still_require_timeout_and_only_one_failure_increment() {
        let helper = peer("carol", 12);
        let (mut probes, mut health, attempt, target) = begin(vec![helper.clone()]);
        probes
            .tick(Duration::from_millis(200), &mut health)
            .unwrap();
        probes
            .dispatched(attempt, &helper, Duration::from_millis(200))
            .unwrap();
        probes
            .reply(
                attempt,
                &target,
                &helper,
                false,
                Duration::from_millis(300),
                &mut health,
            )
            .unwrap();
        assert!(
            probes
                .tick(Duration::from_millis(999), &mut health)
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            outcome(probes.tick(Duration::from_secs(1), &mut health).unwrap()),
            Outcome::Failed
        );
        assert_eq!(health.score(), 1);
    }

    #[test]
    fn queued_or_unscheduled_work_is_local_degradation_not_remote_failure() {
        let mut probes = Probes::new(timing(), 1, 3).unwrap();
        let mut health = Health::default();
        probes
            .start(peer("bob", 11), Vec::new(), false, Duration::ZERO, health)
            .unwrap();
        assert_eq!(
            outcome(probes.tick(Duration::from_secs(1), &mut health).unwrap()),
            Outcome::LocalDegradation
        );
        assert_eq!(health.score(), 0);
        let (mut probes, mut health, _, _) = begin(vec![peer("carol", 12)]);
        assert_eq!(
            outcome(probes.tick(Duration::from_secs(1), &mut health).unwrap()),
            Outcome::LocalDegradation
        );
        assert_eq!(health.score(), 0);
    }

    #[test]
    fn equal_deadline_ack_times_out_and_old_target_pair_is_ignored() {
        let (mut probes, mut health, attempt, target) = begin(Vec::new());
        let mut stale = target.clone();
        stale.version.incarnation += 1;
        assert!(
            probes
                .reply(attempt, &stale, &target, true, Duration::ZERO, &mut health)
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            outcome(
                probes
                    .reply(
                        attempt,
                        &target,
                        &target,
                        true,
                        Duration::from_secs(1),
                        &mut health
                    )
                    .unwrap()
            ),
            Outcome::Failed
        );
    }

    #[test]
    fn cancellation_limits_and_overflow_preserve_capacity_and_health() {
        let mut probes = Probes::new(timing(), 1, 1).unwrap();
        let health = Health::default();
        assert_eq!(
            probes.start(
                peer("bob", 11),
                vec![peer("bob", 11)],
                false,
                Duration::ZERO,
                health
            ),
            Err(Error::Unauthorized)
        );
        assert_eq!(
            probes.start(peer("bob", 11), Vec::new(), false, Duration::MAX, health),
            Err(Error::Overflow)
        );
        let mut probes = Probes::new(timing(), 1, 1).unwrap();
        probes
            .start(peer("bob", 11), Vec::new(), false, Duration::ZERO, health)
            .unwrap();
        assert_eq!(
            probes.start(peer("carol", 12), Vec::new(), false, Duration::ZERO, health),
            Err(Error::Capacity)
        );
        assert_eq!(
            outcome(probes.cancel(1, Duration::ZERO).unwrap()),
            Outcome::LocalDegradation
        );
        assert!(probes.attempts.is_empty());
        assert_eq!(health.score(), 0);
    }

    #[test]
    fn both_timeouts_scale_from_frozen_health() {
        let mut probes = Probes::new(timing(), 1, 0).unwrap();
        let mut health = Health::default();
        health.failure(false, 8);
        probes
            .start(peer("bob", 11), Vec::new(), false, Duration::ZERO, health)
            .unwrap();
        probes
            .dispatched(1, &peer("bob", 11), Duration::ZERO)
            .unwrap();
        let current = &probes.attempts[&1];
        assert_eq!(current.direct_deadline, Some(Duration::from_millis(400)));
        assert_eq!(current.deadline, Some(Duration::from_secs(2)));
    }

    #[test]
    fn schedule_is_reproducible_fair_and_preserves_waiting_members_during_churn() {
        let original: Vec<String> = ["local", "bob", "carol", "dave"]
            .into_iter()
            .map(String::from)
            .collect();
        let mut first = Schedule::new("local".into(), 8).unwrap();
        let mut second = Schedule::new("local".into(), 8).unwrap();
        let trace: Vec<_> = (0..6)
            .map(|_| first.next(&original, |_| 0).unwrap().unwrap())
            .collect();
        let repeated: Vec<_> = (0..6)
            .map(|_| second.next(&original, |_| 0).unwrap().unwrap())
            .collect();
        assert_eq!(trace, repeated);
        assert_eq!(trace[..3].iter().collect::<BTreeSet<_>>().len(), 3);
        assert_eq!(trace[3..].iter().collect::<BTreeSet<_>>().len(), 3);
        let mut churned = original.clone();
        churned.push("erin".into());
        let mut scheduler = Schedule::new("local".into(), 8).unwrap();
        scheduler.next(&original, |_| 0).unwrap();
        let waiting: Vec<_> = scheduler.remaining.iter().cloned().collect();
        for expected in waiting {
            assert_eq!(scheduler.next(&churned, |_| 0).unwrap(), Some(expected));
        }
        assert_eq!(scheduler.next(&[], |_| 0).unwrap(), None);
        assert_eq!(
            scheduler.next(&original, |upper| upper),
            Err(Error::InvalidConfig)
        );
        assert!(scheduler.remaining.is_empty());
    }
}
