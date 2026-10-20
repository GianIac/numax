use std::collections::BTreeMap;
use std::time::Duration;

use super::probe::{self, Outcome, Peer, Probes, Schedule};
use super::suspicion::{Health, Suspicion, Timing};
use super::{Error, Version};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Status {
    Alive,
    Suspect,
    Dead,
    Left,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Update {
    pub(super) member: String,
    pub(super) version: Version,
    pub(super) status: Status,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Action {
    PersistEpoch { token: u64, epoch: u64 },
    PersistFloor { token: u64, update: Update },
    Publish(Update),
    Buddy { member: String, version: Version },
    Suspicion { update: Update, reporter: String },
    Probe(probe::Action),
}

#[derive(Debug)]
struct Member {
    version: Version,
    status: Status,
    session: u64,
    suspicion: Option<Suspicion>,
    pending: Option<(u64, Status)>,
    closed_epoch: Option<u64>,
}

pub(super) struct Membership {
    local: String,
    version: Option<Version>,
    starting: Option<(u64, u64)>,
    leaving: bool,
    leave_token: Option<u64>,
    fenced: bool,
    members: BTreeMap<String, Member>,
    capacity: usize,
    timing: Timing,
    health: Health,
    probes: Probes,
    schedule: Schedule,
    serial: u64,
    now: Duration,
    next_probe_at: Duration,
}

impl Membership {
    pub(super) fn local_degradation(&mut self) -> Vec<Action> {
        self.probes
            .invalidate(None)
            .into_iter()
            .map(Action::Probe)
            .collect()
    }

    #[cfg(test)]
    pub(super) fn snapshot(&self) -> Vec<Update> {
        self.members
            .iter()
            .map(|(member, record)| Update {
                member: member.clone(),
                version: record.version,
                status: record.status,
            })
            .collect()
    }

    pub(super) fn health(&self) -> u32 {
        self.health.score()
    }

    pub(super) fn local_id(&self) -> &str {
        &self.local
    }

    pub(super) fn capacity(&self) -> usize {
        self.capacity
    }

    pub(super) fn local_epoch(&self) -> Option<u64> {
        self.version.map(|version| version.epoch)
    }

    pub(super) fn fence(&mut self) {
        self.fenced = true;
    }

    pub(super) fn pending_floor(&self, action: &Action) -> bool {
        if self.fenced {
            return false;
        }
        let Action::PersistFloor { token, update } = action else {
            return false;
        };
        if update.member == self.local {
            return self.leave_token == Some(*token)
                && self.version == Some(update.version)
                && update.status == Status::Left;
        }
        self.members.get(&update.member).is_some_and(|member| {
            member.version == update.version && member.pending == Some((*token, update.status))
        })
    }

    pub(super) fn restore(&mut self, floors: &[super::schema::Floor]) -> Result<(), Error> {
        if self.version.is_some()
            || self.starting.is_some()
            || self.fenced
            || !self.members.is_empty()
        {
            return Err(Error::NotReady);
        }
        if floors.len() > self.capacity {
            return Err(Error::Capacity);
        }
        let mut restored = BTreeMap::new();
        for floor in floors {
            floor.validate()?;
            if floor.member == self.local || restored.contains_key(&floor.member) {
                return Err(Error::InvalidMetadata);
            }
            restored.insert(
                floor.member.clone(),
                Member {
                    version: floor.version,
                    status: floor.status,
                    session: 0,
                    suspicion: None,
                    pending: None,
                    closed_epoch: floor.closed_epoch,
                },
            );
        }
        self.members = restored;
        Ok(())
    }

    pub(super) fn new(
        local: String,
        capacity: usize,
        timing: Timing,
        active_probes: usize,
        helpers: usize,
    ) -> Result<Self, Error> {
        Self::validate_id(&local)?;
        if capacity == 0 || capacity > 4096 {
            return Err(Error::InvalidConfig);
        }
        let schedule = Schedule::new(local.clone(), capacity)?;
        let probes = Probes::new(timing, active_probes, helpers)?;
        Ok(Self {
            local,
            version: None,
            starting: None,
            leaving: false,
            leave_token: None,
            fenced: false,
            members: BTreeMap::new(),
            capacity,
            timing: timing.validate()?,
            health: Health::default(),
            probes,
            schedule,
            serial: 0,
            now: Duration::ZERO,
            next_probe_at: Duration::ZERO,
        })
    }

    fn validate_id(member: &str) -> Result<(), Error> {
        if member.is_empty() || member.len() > 256 {
            return Err(Error::Capacity);
        }
        Ok(())
    }

    fn advance(&mut self, now: Duration) -> Result<(), Error> {
        if now < self.now {
            return Err(Error::ClockRegression);
        }
        self.now = now;
        Ok(())
    }

    fn token(&mut self) -> Result<u64, Error> {
        self.serial = self.serial.checked_add(1).ok_or(Error::Overflow)?;
        Ok(self.serial)
    }

    fn ready(&self) -> Result<Version, Error> {
        if self.fenced || self.leaving {
            return Err(Error::NotReady);
        }
        self.version.ok_or(Error::NotReady)
    }

    pub(super) fn start(&mut self, previous_epoch: u64) -> Result<Action, Error> {
        if self.fenced || self.starting.is_some() || self.version.is_some() {
            return Err(Error::NotReady);
        }
        let epoch = previous_epoch.checked_add(1).ok_or(Error::Overflow)?;
        let token = self.token()?;
        self.starting = Some((token, epoch));
        Ok(Action::PersistEpoch { token, epoch })
    }

    pub(super) fn confirm_start(
        &mut self,
        token: u64,
        flushed: bool,
    ) -> Result<Vec<Action>, Error> {
        if self.fenced {
            return Err(Error::NotReady);
        }
        let Some((expected, epoch)) = self.starting else {
            return Ok(Vec::new());
        };
        if token != expected {
            return Ok(Vec::new());
        }
        self.starting = None;
        if !flushed {
            self.fenced = true;
            return Err(Error::Persistence);
        }
        let version = Version {
            epoch,
            incarnation: 0,
        };
        self.version = Some(version);
        Ok(vec![Action::Publish(Update {
            member: self.local.clone(),
            version,
            status: Status::Alive,
        })])
    }

    pub(super) fn admit(
        &mut self,
        member: &str,
        version: Version,
        session: u64,
        now: Duration,
    ) -> Result<Vec<Action>, Error> {
        self.advance(now)?;
        self.ready()?;
        Self::validate_id(member)?;
        if member == self.local || version.epoch == 0 || session == 0 {
            return Err(Error::Unauthorized);
        }
        if let Some(current) = self.members.get(member) {
            if current
                .closed_epoch
                .is_some_and(|epoch| version.epoch <= epoch)
                || (current
                    .pending
                    .is_some_and(|(_, status)| status == Status::Left)
                    && version.epoch <= current.version.epoch)
                || version < current.version
                || (version == current.version && current.status != Status::Alive)
            {
                return Ok(Vec::new());
            }
        } else if self.members.len() >= self.capacity {
            return Err(Error::Capacity);
        }
        if self.members.get(member).is_some_and(|current| {
            current.version == version
                && current.session == session
                && current.status == Status::Alive
        }) {
            return Ok(Vec::new());
        }
        let closed_epoch = self
            .members
            .get(member)
            .and_then(|current| current.closed_epoch);
        let mut actions: Vec<_> = self
            .probes
            .invalidate(Some(member))
            .into_iter()
            .map(Action::Probe)
            .collect();
        self.members.insert(
            member.to_owned(),
            Member {
                version,
                status: Status::Alive,
                session,
                suspicion: None,
                pending: None,
                closed_epoch,
            },
        );
        actions.push(Action::Publish(Update {
            member: member.into(),
            version,
            status: Status::Alive,
        }));
        Ok(actions)
    }

    fn authorized(&self, member: &str, session: u64) -> Result<(), Error> {
        if self
            .members
            .get(member)
            .is_some_and(|record| record.session == session && session != 0)
        {
            Ok(())
        } else {
            Err(Error::Unauthorized)
        }
    }

    pub(super) fn accepted_owner(&self, member: &str, version: Version, session: u64) -> bool {
        session != 0
            && self.members.get(member).is_some_and(|current| {
                current.session == session
                    && current.version == version
                    && current.status == Status::Alive
                    && current.pending.is_none()
            })
    }

    pub(super) fn owner_alive(
        &mut self,
        member: &str,
        session: u64,
        version: Version,
        now: Duration,
    ) -> Result<Vec<Action>, Error> {
        self.authorized(member, session)?;
        self.admit(member, version, session, now)
    }

    pub(super) fn suspect(
        &mut self,
        reporter: &str,
        session: u64,
        target: &str,
        version: Version,
        now: Duration,
    ) -> Result<Vec<Action>, Error> {
        self.authorized(reporter, session)?;
        if reporter == target {
            return Err(Error::Unauthorized);
        }
        self.begin_suspicion(reporter, target, version, now)
    }

    pub(super) fn probe_failed(
        &mut self,
        target: &str,
        version: Version,
        now: Duration,
    ) -> Result<Vec<Action>, Error> {
        let reporter = self.local.clone();
        self.begin_suspicion(&reporter, target, version, now)
    }

    fn begin_suspicion(
        &mut self,
        reporter: &str,
        target: &str,
        version: Version,
        now: Duration,
    ) -> Result<Vec<Action>, Error> {
        self.advance(now)?;
        let local_version = self.ready()?;
        if target == self.local {
            if version.epoch != local_version.epoch || version < local_version {
                return Ok(Vec::new());
            }
            let newer = local_version.refute(version)?;
            self.version = Some(newer);
            self.health.self_refutation(self.timing.health_max);
            return Ok(vec![Action::Publish(Update {
                member: self.local.clone(),
                version: newer,
                status: Status::Alive,
            })]);
        }
        let current = self.members.get(target).ok_or(Error::Unauthorized)?;
        if version != current.version
            || matches!(current.status, Status::Dead | Status::Left)
            || current
                .pending
                .is_some_and(|(_, status)| status == Status::Left)
        {
            return Ok(Vec::new());
        }
        if current.suspicion.is_some() {
            let current = self.members.get_mut(target).ok_or(Error::Unauthorized)?;
            let suspicion = current.suspicion.as_mut().ok_or(Error::NotReady)?;
            if !suspicion.confirm(reporter, now)? {
                return Ok(Vec::new());
            }
            let expired = (now >= suspicion.deadline).then_some(suspicion.timer);
            let mut actions = vec![Action::Suspicion {
                update: Update {
                    member: target.into(),
                    version,
                    status: Status::Suspect,
                },
                reporter: reporter.into(),
            }];
            if let Some(timer) = expired {
                actions.extend(self.expire(target, version, timer, now)?);
            }
            return Ok(actions);
        }
        let available = self
            .members
            .iter()
            .filter(|(member, record)| {
                member.as_str() != target
                    && member.as_str() != reporter
                    && matches!(record.status, Status::Alive | Status::Suspect)
            })
            .count()
            + usize::from(reporter != self.local);
        let timer = self.token()?;
        let suspicion =
            Suspicion::new(version, timer, reporter.into(), now, self.timing, available)?;
        let current = self.members.get_mut(target).ok_or(Error::Unauthorized)?;
        current.status = Status::Suspect;
        current.suspicion = Some(suspicion);
        Ok(vec![
            Action::Suspicion {
                update: Update {
                    member: target.into(),
                    version,
                    status: Status::Suspect,
                },
                reporter: reporter.into(),
            },
            Action::Buddy {
                member: target.into(),
                version,
            },
        ])
    }

    pub(super) fn expire(
        &mut self,
        target: &str,
        version: Version,
        timer: u64,
        now: Duration,
    ) -> Result<Vec<Action>, Error> {
        self.advance(now)?;
        self.ready()?;
        let current = self.members.get(target).ok_or(Error::Unauthorized)?;
        if current.pending.is_some()
            || !current.suspicion.as_ref().is_some_and(|suspicion| {
                suspicion.version == version
                    && suspicion.timer == timer
                    && now >= suspicion.deadline
            })
        {
            return Ok(Vec::new());
        }
        self.request_floor(target, Status::Dead)
    }

    fn request_floor(&mut self, target: &str, status: Status) -> Result<Vec<Action>, Error> {
        let token = self.token()?;
        let current = self.members.get_mut(target).ok_or(Error::Unauthorized)?;
        current.pending = Some((token, status));
        if status == Status::Left {
            current.closed_epoch = Some(current.version.epoch);
        }
        Ok(vec![Action::PersistFloor {
            token,
            update: Update {
                member: target.into(),
                version: current.version,
                status,
            },
        }])
    }

    pub(super) fn owner_leave(
        &mut self,
        member: &str,
        session: u64,
        version: Version,
        now: Duration,
    ) -> Result<Vec<Action>, Error> {
        self.advance(now)?;
        self.ready()?;
        self.authorized(member, session)?;
        let current = self.members.get(member).ok_or(Error::Unauthorized)?;
        if version != current.version
            || current.status == Status::Left
            || current
                .pending
                .is_some_and(|(_, status)| status == Status::Left)
        {
            return Ok(Vec::new());
        }
        self.request_floor(member, Status::Left)
    }

    pub(super) fn report_dead(
        &mut self,
        reporter: &str,
        session: u64,
        target: &str,
        version: Version,
        now: Duration,
    ) -> Result<Vec<Action>, Error> {
        self.report_terminal(reporter, session, target, version, Status::Dead, now)
    }

    pub(super) fn relay_leave(
        &mut self,
        reporter: &str,
        session: u64,
        target: &str,
        version: Version,
        now: Duration,
    ) -> Result<Vec<Action>, Error> {
        if target == self.local {
            return Err(Error::Unauthorized);
        }
        self.report_terminal(reporter, session, target, version, Status::Left, now)
    }

    fn report_terminal(
        &mut self,
        reporter: &str,
        session: u64,
        target: &str,
        version: Version,
        status: Status,
        now: Duration,
    ) -> Result<Vec<Action>, Error> {
        self.advance(now)?;
        self.ready()?;
        self.authorized(reporter, session)?;
        if reporter == target {
            return Err(Error::Unauthorized);
        }
        if target == self.local {
            return self.begin_suspicion(reporter, target, version, now);
        }
        let current = self.members.get(target).ok_or(Error::Unauthorized)?;
        if current.version != version
            || current.pending.is_some()
            || current.status == Status::Left
            || current.status == Status::Dead && status == Status::Dead
        {
            return Ok(Vec::new());
        }
        self.request_floor(target, status)
    }

    pub(super) fn leave(&mut self) -> Result<Vec<Action>, Error> {
        let version = self.ready()?;
        let token = self.token()?;
        self.leaving = true;
        self.leave_token = Some(token);
        let mut actions: Vec<_> = self
            .probes
            .invalidate(None)
            .into_iter()
            .map(Action::Probe)
            .collect();
        actions.push(Action::PersistFloor {
            token,
            update: Update {
                member: self.local.clone(),
                version,
                status: Status::Left,
            },
        });
        Ok(actions)
    }

    pub(super) fn confirm_floor(
        &mut self,
        token: u64,
        flushed: bool,
    ) -> Result<Vec<Action>, Error> {
        if self.fenced {
            return Err(Error::NotReady);
        }
        if self.leave_token == Some(token) {
            self.leave_token = None;
            if !flushed {
                self.fenced = true;
                return Err(Error::Persistence);
            }
            return Ok(vec![Action::Publish(Update {
                member: self.local.clone(),
                version: self.version.ok_or(Error::NotReady)?,
                status: Status::Left,
            })]);
        }
        let Some((member, current)) = self.members.iter_mut().find(|(_, record)| {
            record
                .pending
                .is_some_and(|(expected, _)| expected == token)
        }) else {
            return Ok(Vec::new());
        };
        let (_, status) = current.pending.take().ok_or(Error::NotReady)?;
        if !flushed {
            return Err(Error::Persistence);
        }
        current.status = status;
        current.suspicion = None;
        if status == Status::Left {
            current.closed_epoch = Some(current.version.epoch);
        }
        let update = Update {
            member: member.clone(),
            version: current.version,
            status,
        };
        let mut actions: Vec<_> = self
            .probes
            .invalidate(Some(&update.member))
            .into_iter()
            .map(Action::Probe)
            .collect();
        actions.push(Action::Publish(update));
        Ok(actions)
    }

    pub(super) fn probe_next(
        &mut self,
        now: Duration,
        mut choose: impl FnMut(usize) -> usize,
    ) -> Result<Vec<Action>, Error> {
        self.advance(now)?;
        self.ready()?;
        if now < self.next_probe_at {
            return Ok(Vec::new());
        }
        if self.probes.is_full() {
            return Err(Error::Capacity);
        }
        let next_probe_at = super::deadline(now, self.health.scale(self.timing.interval)?)?;
        let eligible: Vec<_> = self
            .members
            .iter()
            .filter(|(_, record)| {
                matches!(record.status, Status::Alive | Status::Suspect) && record.pending.is_none()
            })
            .map(|(id, _)| id.clone())
            .collect();
        let Some(target_id) = self.schedule.next(&eligible, &mut choose)? else {
            return Ok(Vec::new());
        };
        let peer = |id: &String| Peer {
            id: id.clone(),
            version: self.members[id].version,
            session: self.members[id].session,
        };
        let target = peer(&target_id);
        let mut helpers: Vec<_> = eligible
            .iter()
            .filter(|id| **id != target_id && self.members[*id].status == Status::Alive)
            .map(peer)
            .collect();
        for upper in (2..=helpers.len()).rev() {
            let index = choose(upper);
            if index >= upper {
                return Err(Error::InvalidConfig);
            }
            helpers.swap(upper - 1, index);
        }
        helpers.truncate(self.probes.helper_limit());
        let buddy = self.members[&target_id].status == Status::Suspect;
        let actions = self
            .probes
            .start(target, helpers, buddy, now, self.health)?;
        self.next_probe_at = next_probe_at;
        Ok(actions.into_iter().map(Action::Probe).collect())
    }

    pub(super) fn probe_dispatched(
        &mut self,
        attempt: u64,
        route: &Peer,
        now: Duration,
    ) -> Result<Vec<Action>, Error> {
        self.advance(now)?;
        self.ready()?;
        Ok(self
            .probes
            .dispatched(attempt, route, now)?
            .into_iter()
            .map(Action::Probe)
            .collect())
    }

    pub(super) fn probe_reply(
        &mut self,
        attempt: u64,
        target: &Peer,
        route: &Peer,
        ack: bool,
        now: Duration,
    ) -> Result<Vec<Action>, Error> {
        self.advance(now)?;
        self.ready()?;
        let actions = self
            .probes
            .reply(attempt, target, route, ack, now, &mut self.health)?;
        self.apply_probe_actions(actions, now)
    }

    pub(super) fn probe_send_failed(
        &mut self,
        attempt: u64,
        now: Duration,
    ) -> Result<Vec<Action>, Error> {
        self.advance(now)?;
        self.ready()?;
        Ok(self
            .probes
            .cancel(attempt, now)?
            .into_iter()
            .map(Action::Probe)
            .collect())
    }

    pub(super) fn tick(&mut self, now: Duration) -> Result<Vec<Action>, Error> {
        self.advance(now)?;
        self.ready()?;
        let probe_actions = self.probes.tick(now, &mut self.health)?;
        let mut actions = self.apply_probe_actions(probe_actions, now)?;
        let due: Vec<_> = self
            .members
            .iter()
            .filter_map(|(id, record)| {
                record
                    .suspicion
                    .as_ref()
                    .filter(|suspicion| now >= suspicion.deadline)
                    .map(|suspicion| (id.clone(), suspicion.version, suspicion.timer))
            })
            .collect();
        for (target, version, timer) in due {
            actions.extend(self.expire(&target, version, timer, now)?);
        }
        Ok(actions)
    }

    fn apply_probe_actions(
        &mut self,
        probes: Vec<probe::Action>,
        now: Duration,
    ) -> Result<Vec<Action>, Error> {
        let mut actions = Vec::new();
        for action in probes {
            if let probe::Action::Finished {
                target,
                outcome: Outcome::Failed,
                ..
            } = &action
                && self.members.get(&target.id).is_some_and(|current| {
                    current.version == target.version && current.session == target.session
                })
            {
                actions.extend(self.probe_failed(&target.id, target.version, now)?);
            }
            actions.push(Action::Probe(action));
        }
        Ok(actions)
    }
}

#[cfg(test)]
mod tests {
    use super::super::suspicion::tests::timing;
    use super::*;

    fn version(epoch: u64, incarnation: u64) -> Version {
        Version { epoch, incarnation }
    }

    fn machine() -> Membership {
        let mut machine = Membership::new("local".into(), 4, timing(), 1, 3).unwrap();
        let Action::PersistEpoch { token, .. } = machine.start(0).unwrap() else {
            panic!()
        };
        machine.confirm_start(token, true).unwrap();
        machine
            .admit("bob", version(1, 0), 11, Duration::ZERO)
            .unwrap();
        machine
            .admit("carol", version(1, 0), 12, Duration::ZERO)
            .unwrap();
        machine
    }

    fn expire_bob(machine: &mut Membership) -> u64 {
        machine
            .probe_failed("bob", version(1, 0), Duration::ZERO)
            .unwrap();
        let suspicion = machine.members["bob"].suspicion.clone().unwrap();
        let actions = machine
            .expire(
                "bob",
                suspicion.version,
                suspicion.timer,
                suspicion.deadline,
            )
            .unwrap();
        let Action::PersistFloor { token, .. } = actions[0] else {
            panic!()
        };
        token
    }

    #[test]
    fn startup_requires_covering_flush_and_failed_flush_fences_announcements() {
        let mut machine = Membership::new("local".into(), 2, timing(), 1, 3).unwrap();
        assert_eq!(
            machine.admit("bob", version(1, 0), 11, Duration::ZERO),
            Err(Error::NotReady)
        );
        let Action::PersistEpoch { token, epoch } = machine.start(7).unwrap() else {
            panic!()
        };
        assert_eq!(epoch, 8);
        assert!(machine.confirm_start(token + 1, true).unwrap().is_empty());
        assert_eq!(machine.confirm_start(token, false), Err(Error::Persistence));
        assert_eq!(machine.start(7), Err(Error::NotReady));
        assert!(machine.version.is_none());
    }

    #[test]
    fn cooperative_leave_relay_requires_admitted_reporter_and_matching_subject() {
        let mut machine = machine();
        assert_eq!(
            machine.relay_leave("carol", 99, "bob", version(1, 0), Duration::ZERO),
            Err(Error::Unauthorized)
        );
        assert_eq!(
            machine.relay_leave("carol", 12, "local", version(1, 0), Duration::ZERO),
            Err(Error::Unauthorized)
        );
        assert!(
            machine
                .relay_leave("carol", 12, "bob", version(2, 0), Duration::ZERO)
                .unwrap()
                .is_empty()
        );
        let actions = machine
            .relay_leave("carol", 12, "bob", version(1, 0), Duration::ZERO)
            .unwrap();
        assert_eq!(machine.members["bob"].status, Status::Alive);
        let Action::PersistFloor { token, .. } = actions[0] else {
            panic!()
        };
        assert!(matches!(
            machine.confirm_floor(token, true).unwrap().last(),
            Some(Action::Publish(Update {
                status: Status::Left,
                ..
            }))
        ));
        assert!(
            machine
                .admit("bob", version(1, 99), 11, Duration::ZERO)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn newer_alive_invalidates_timer_and_pending_floor_confirmation() {
        let mut machine = machine();
        let token = expire_bob(&mut machine);
        let suspicion = machine.members["bob"].suspicion.clone().unwrap();
        machine
            .owner_alive("bob", 11, version(1, 1), suspicion.deadline)
            .unwrap();
        assert!(machine.confirm_floor(token, true).unwrap().is_empty());
        assert!(
            machine
                .expire(
                    "bob",
                    suspicion.version,
                    suspicion.timer,
                    suspicion.deadline
                )
                .unwrap()
                .is_empty()
        );
        assert_eq!(machine.members["bob"].status, Status::Alive);
    }

    #[test]
    fn terminal_state_waits_for_persistence_and_failure_can_retry_without_publishing() {
        let mut machine = machine();
        let token = expire_bob(&mut machine);
        assert_eq!(machine.members["bob"].status, Status::Suspect);
        assert_eq!(machine.confirm_floor(token, false), Err(Error::Persistence));
        let suspicion = machine.members["bob"].suspicion.clone().unwrap();
        let actions = machine
            .expire(
                "bob",
                suspicion.version,
                suspicion.timer,
                suspicion.deadline,
            )
            .unwrap();
        let Action::PersistFloor { token, .. } = actions[0] else {
            panic!()
        };
        assert_eq!(machine.confirm_floor(token, true).unwrap().len(), 1);
        assert_eq!(machine.members["bob"].status, Status::Dead);
        assert!(
            machine
                .owner_alive("bob", 11, version(1, 0), suspicion.deadline)
                .unwrap()
                .is_empty()
        );
        machine
            .owner_alive("bob", 11, version(1, 1), suspicion.deadline)
            .unwrap();
        assert_eq!(machine.members["bob"].status, Status::Alive);
    }

    #[test]
    fn left_closes_epoch_even_for_higher_incarnation_and_retains_capacity() {
        let mut machine = machine();
        let actions = machine
            .owner_leave("bob", 11, version(1, 0), Duration::ZERO)
            .unwrap();
        let Action::PersistFloor { token, .. } = actions[0] else {
            panic!()
        };
        machine.confirm_floor(token, true).unwrap();
        assert!(
            machine
                .owner_alive("bob", 11, version(1, 99), Duration::ZERO)
                .unwrap()
                .is_empty()
        );
        assert_eq!(machine.members["bob"].status, Status::Left);
        machine
            .admit("bob", version(2, 0), 13, Duration::ZERO)
            .unwrap();
        assert_eq!(machine.members["bob"].status, Status::Alive);
        assert_eq!(
            machine.owner_alive("bob", 11, version(3, 0), Duration::ZERO),
            Err(Error::Unauthorized)
        );
        assert!(
            machine
                .admit("bob", version(1, 100), 11, Duration::ZERO)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn self_refutation_is_owner_only_checked_and_duplicate_safe() {
        let mut machine = machine();
        let actions = machine
            .suspect("carol", 12, "local", version(1, 3), Duration::ZERO)
            .unwrap();
        assert_eq!(actions.len(), 1);
        assert_eq!(machine.version, Some(version(1, 4)));
        assert_eq!(machine.health.score(), 1);
        assert!(
            machine
                .suspect("carol", 12, "local", version(1, 3), Duration::ZERO)
                .unwrap()
                .is_empty()
        );
        assert_eq!(machine.health.score(), 1);
        assert_eq!(
            machine.suspect("carol", 12, "local", version(1, u64::MAX), Duration::ZERO),
            Err(Error::Overflow)
        );
        assert_eq!(machine.version, Some(version(1, 4)));
        let actions = machine.leave().unwrap();
        let Action::PersistFloor { token, .. } = actions[0] else {
            panic!()
        };
        assert_eq!(
            machine.suspect("carol", 12, "local", version(1, 4), Duration::ZERO),
            Err(Error::NotReady)
        );
        assert_eq!(machine.confirm_floor(token, true).unwrap().len(), 1);
        assert_eq!(machine.start(1), Err(Error::NotReady));
    }

    #[test]
    fn unknown_subjects_unauthorized_sessions_and_future_pairs_cannot_change_members() {
        let mut machine = machine();
        assert_eq!(
            machine.suspect("carol", 99, "bob", version(1, 0), Duration::ZERO),
            Err(Error::Unauthorized)
        );
        assert_eq!(
            machine.suspect("carol", 12, "unknown", version(1, 0), Duration::ZERO),
            Err(Error::Unauthorized)
        );
        assert!(
            machine
                .suspect("carol", 12, "bob", version(2, 0), Duration::ZERO)
                .unwrap()
                .is_empty()
        );
        assert!(
            machine
                .suspect("carol", 12, "bob", version(1, 1), Duration::ZERO)
                .unwrap()
                .is_empty()
        );
        assert_eq!(machine.members["bob"].status, Status::Alive);
        assert_eq!(machine.members.len(), 2);
    }

    #[test]
    fn resource_clock_and_counter_limits_fail_without_evicting_records() {
        let mut machine = machine();
        machine.capacity = 2;
        assert_eq!(
            machine.admit("extra", version(1, 0), 14, Duration::ZERO),
            Err(Error::Capacity)
        );
        assert_eq!(
            machine.admit(&"x".repeat(257), version(1, 0), 14, Duration::ZERO),
            Err(Error::Capacity)
        );
        machine.now = Duration::from_secs(1);
        assert_eq!(
            machine.probe_failed("bob", version(1, 0), Duration::ZERO),
            Err(Error::ClockRegression)
        );
        machine.serial = u64::MAX;
        assert_eq!(
            machine.probe_failed("bob", version(1, 0), Duration::from_secs(1)),
            Err(Error::Overflow)
        );
        assert_eq!(machine.members["bob"].status, Status::Alive);
        assert_eq!(machine.members.len(), 2);
    }

    fn direct(actions: &[Action]) -> (u64, Peer) {
        actions
            .iter()
            .find_map(|action| match action {
                Action::Probe(probe::Action::Direct {
                    attempt, target, ..
                }) => Some((*attempt, target.clone())),
                _ => None,
            })
            .unwrap()
    }

    #[test]
    fn shared_core_runs_probe_suspicion_persistence_and_owner_refutation() {
        let mut machine = machine();
        machine.probes = Probes::new(timing(), 1, 0).unwrap();
        let (attempt, target) = direct(&machine.probe_next(Duration::ZERO, |_| 0).unwrap());
        machine
            .probe_dispatched(attempt, &target, Duration::ZERO)
            .unwrap();
        let actions = machine.tick(Duration::from_secs(1)).unwrap();
        assert!(
            actions.iter().any(
                |action| matches!(action, Action::Buddy { member, .. } if member == &target.id)
            )
        );
        assert_eq!(machine.members[&target.id].status, Status::Suspect);
        assert_eq!(machine.health.score(), 1);
        let suspicion = machine.members[&target.id].suspicion.clone().unwrap();
        let actions = machine.tick(suspicion.deadline).unwrap();
        let token = actions
            .iter()
            .find_map(|action| match action {
                Action::PersistFloor { token, .. } => Some(*token),
                _ => None,
            })
            .unwrap();
        assert_eq!(machine.members[&target.id].status, Status::Suspect);
        machine.confirm_floor(token, true).unwrap();
        assert_eq!(machine.members[&target.id].status, Status::Dead);
        machine
            .owner_alive(
                &target.id,
                target.session,
                version(1, 1),
                suspicion.deadline,
            )
            .unwrap();
        assert_eq!(machine.members[&target.id].status, Status::Alive);
    }

    #[test]
    fn new_session_cancels_old_probe_and_late_reply_cannot_complete_a_new_one() {
        let mut machine = machine();
        let (attempt, target) = direct(&machine.probe_next(Duration::ZERO, |_| 0).unwrap());
        machine
            .probe_dispatched(attempt, &target, Duration::ZERO)
            .unwrap();
        let actions = machine
            .admit(&target.id, target.version, 99, Duration::ZERO)
            .unwrap();
        assert!(actions.iter().any(|action| matches!(
            action,
            Action::Probe(probe::Action::Finished {
                outcome: Outcome::LocalDegradation,
                ..
            })
        )));
        assert!(
            machine
                .probe_reply(attempt, &target, &target, true, Duration::ZERO)
                .unwrap()
                .is_empty()
        );
        assert!(
            machine
                .probe_next(Duration::ZERO, |_| 0)
                .unwrap()
                .is_empty()
        );
        assert_eq!(machine.health.score(), 0);
        assert!(machine.tick(Duration::from_secs(1)).unwrap().is_empty());
    }

    #[test]
    fn explicit_local_send_failure_does_not_start_suspicion_or_penalize_health() {
        let mut machine = machine();
        let (attempt, target) = direct(&machine.probe_next(Duration::ZERO, |_| 0).unwrap());
        let actions = machine.probe_send_failed(attempt, Duration::ZERO).unwrap();
        assert_eq!(actions.len(), 1);
        assert_eq!(machine.members[&target.id].status, Status::Alive);
        assert_eq!(machine.health.score(), 0);
        assert!(machine.tick(Duration::from_secs(1)).unwrap().is_empty());
    }

    #[test]
    fn pending_owner_leave_cannot_be_reopened_by_same_epoch_alive_or_suspect() {
        let mut machine = machine();
        let actions = machine
            .owner_leave("bob", 11, version(1, 0), Duration::ZERO)
            .unwrap();
        let Action::PersistFloor { token, .. } = actions[0] else {
            panic!()
        };
        assert!(
            machine
                .owner_alive("bob", 11, version(1, 99), Duration::ZERO)
                .unwrap()
                .is_empty()
        );
        assert!(
            machine
                .suspect("carol", 12, "bob", version(1, 0), Duration::ZERO)
                .unwrap()
                .is_empty()
        );
        assert!(
            machine
                .report_dead("carol", 12, "bob", version(1, 0), Duration::ZERO)
                .unwrap()
                .is_empty()
        );
        machine.confirm_floor(token, true).unwrap();
        assert_eq!(machine.members["bob"].status, Status::Left);
    }

    #[test]
    fn same_pair_failure_precedence_and_owner_refutation_cover_all_states() {
        for status in [Status::Alive, Status::Suspect, Status::Dead, Status::Left] {
            let mut machine = machine();
            machine.members.get_mut("bob").unwrap().status = status;
            if status == Status::Left {
                machine.members.get_mut("bob").unwrap().closed_epoch = Some(1);
            }
            let actions = machine
                .report_dead("carol", 12, "bob", version(1, 0), Duration::ZERO)
                .unwrap();
            if matches!(status, Status::Alive | Status::Suspect) {
                let Action::PersistFloor { token, .. } = actions[0] else {
                    panic!()
                };
                machine.confirm_floor(token, true).unwrap();
                assert_eq!(machine.members["bob"].status, Status::Dead);
            } else {
                assert!(actions.is_empty());
            }
            machine
                .owner_alive("bob", 11, version(1, 1), Duration::ZERO)
                .unwrap();
            assert_eq!(
                machine.members["bob"].status,
                if status == Status::Left {
                    Status::Left
                } else {
                    Status::Alive
                }
            );
            machine
                .admit("bob", version(2, 0), 20, Duration::ZERO)
                .unwrap();
            assert_eq!(machine.members["bob"].version, version(2, 0));
            assert!(
                machine
                    .suspect("carol", 12, "bob", version(1, 100), Duration::ZERO)
                    .unwrap()
                    .is_empty()
            );
        }
    }

    #[test]
    fn corroboration_past_shortened_deadline_requests_floor_immediately() {
        let mut machine = machine();
        machine
            .probe_failed("bob", version(1, 0), Duration::ZERO)
            .unwrap();
        let actions = machine
            .suspect("carol", 12, "bob", version(1, 0), Duration::from_secs(3))
            .unwrap();
        assert!(matches!(&actions[0], Action::Suspicion { reporter, .. } if reporter == "carol"));
        assert!(
            actions
                .iter()
                .any(|action| matches!(action, Action::PersistFloor { .. }))
        );
        assert_eq!(machine.members["bob"].status, Status::Suspect);
    }

    #[test]
    fn leave_cancels_owned_probes_and_empty_membership_has_no_probe() {
        let mut machine = machine();
        let (attempt, target) = direct(&machine.probe_next(Duration::ZERO, |_| 0).unwrap());
        let actions = machine.leave().unwrap();
        assert_eq!(actions.len(), 2);
        assert!(matches!(
            &actions[0],
            Action::Probe(probe::Action::Finished {
                outcome: Outcome::LocalDegradation,
                ..
            })
        ));
        assert_eq!(
            machine.probe_reply(attempt, &target, &target, true, Duration::ZERO),
            Err(Error::NotReady)
        );
        let mut empty = Membership::new("local".into(), 1, timing(), 1, 0).unwrap();
        let Action::PersistEpoch { token, .. } = empty.start(0).unwrap() else {
            panic!()
        };
        empty.confirm_start(token, true).unwrap();
        assert!(empty.probe_next(Duration::ZERO, |_| 0).unwrap().is_empty());
    }

    #[test]
    fn uncertain_leave_flush_keeps_epoch_closed_and_can_retry_persistence() {
        let mut machine = machine();
        let actions = machine
            .owner_leave("bob", 11, version(1, 0), Duration::ZERO)
            .unwrap();
        let Action::PersistFloor { token, .. } = actions[0] else {
            panic!()
        };
        assert_eq!(machine.confirm_floor(token, false), Err(Error::Persistence));
        assert!(
            machine
                .owner_alive("bob", 11, version(1, 99), Duration::ZERO)
                .unwrap()
                .is_empty()
        );
        let actions = machine
            .owner_leave("bob", 11, version(1, 0), Duration::ZERO)
            .unwrap();
        let Action::PersistFloor { token, .. } = actions[0] else {
            panic!()
        };
        machine.confirm_floor(token, true).unwrap();
        assert_eq!(machine.members["bob"].status, Status::Left);
    }

    #[test]
    fn restored_floors_reject_replay_without_restoring_session_authority() {
        let mut machine = Membership::new("alice".into(), 3, timing(), 2, 1).unwrap();
        machine
            .restore(&[super::super::schema::Floor::from_update(&Update {
                member: "bob".into(),
                version: version(7, 2),
                status: Status::Left,
            })
            .unwrap()])
            .unwrap();
        let Action::PersistEpoch { token, .. } = machine.start(2).unwrap() else {
            panic!()
        };
        machine.confirm_start(token, true).unwrap();
        assert_eq!(
            machine.owner_alive("bob", 0, version(8, 0), Duration::ZERO),
            Err(Error::Unauthorized)
        );
        assert!(
            machine
                .admit("bob", version(7, 99), 11, Duration::ZERO)
                .unwrap()
                .is_empty()
        );
        assert!(
            machine
                .probe_next(Duration::ZERO, |_| 0)
                .unwrap()
                .is_empty()
        );
        assert!(
            !machine
                .admit("bob", version(8, 0), 12, Duration::ZERO)
                .unwrap()
                .is_empty()
        );
        assert_eq!(machine.restore(&[]), Err(Error::NotReady));
    }

    #[test]
    fn fencing_prevents_late_persistence_confirmations_from_publishing() {
        let mut running = machine();
        let actions = running
            .owner_leave("bob", 11, version(1, 0), Duration::ZERO)
            .unwrap();
        let Action::PersistFloor { token, .. } = actions[0] else {
            panic!()
        };
        running.fence();
        assert_eq!(running.confirm_floor(token, true), Err(Error::NotReady));
        let mut starting = Membership::new("alice".into(), 3, timing(), 2, 1).unwrap();
        let Action::PersistEpoch { token, .. } = starting.start(0).unwrap() else {
            panic!()
        };
        starting.fence();
        assert_eq!(starting.confirm_start(token, true), Err(Error::NotReady));
    }

    #[test]
    fn deterministic_event_and_choice_traces_emit_identical_actions() {
        let trace = |choices: &[usize]| {
            let mut machine = machine();
            let mut choices = choices.iter().copied().cycle();
            let mut result = Vec::new();
            for second in 0..4 {
                let now = Duration::from_secs(second);
                let actions = machine
                    .probe_next(now, |upper| choices.next().unwrap() % upper)
                    .unwrap();
                let (attempt, target) = direct(&actions);
                result.extend(actions);
                result.extend(machine.probe_dispatched(attempt, &target, now).unwrap());
                result.extend(
                    machine
                        .probe_reply(attempt, &target, &target, true, now)
                        .unwrap(),
                );
            }
            result
        };
        assert_eq!(trace(&[0, 1, 3, 2]), trace(&[0, 1, 3, 2]));
        assert_ne!(trace(&[0]), trace(&[1]));
    }
}
