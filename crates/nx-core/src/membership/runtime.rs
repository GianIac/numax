use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use nx_net::{
    ControlIncoming, ControlKind, ControlReply, ControlRequest, MembershipRecord, MembershipState,
    MembershipVersion, Node,
};
use nx_store::Store;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::{JoinHandle, JoinSet};

use super::probe::{self, Peer};
use super::state::{Action, Membership, Status, Update};
use super::storage::MembershipStore;
use super::suspicion::Timing;
use super::{Error, Version};
use crate::observability::RuntimeMetrics;
use crate::peer_metrics::{Counter as Metric, Gauge};

const TICK: Duration = Duration::from_millis(25);
const CAPACITY: usize = 4096;
const RPC_BUDGET: Duration = Duration::from_millis(500);
#[cfg(test)]
const SCHEDULING_SAMPLES_LIMIT: usize = 2048;

#[derive(Debug, Clone)]
pub(crate) struct ProbeTiming {
    pub(crate) member: String,
    pub(crate) version: MembershipVersion,
    pub(crate) timing: nx_net::ControlTiming,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct Snapshot {
    pub(crate) records: Vec<MembershipRecord>,
    pub(crate) probe_timings: Vec<ProbeTiming>,
    pub(crate) health: u32,
    pub(crate) error: Option<String>,
    pub(crate) successful_probes: u64,
    pub(crate) failed_probes: u64,
    #[cfg(test)]
    pub(crate) tick_lateness_micros: Vec<u64>,
}

pub(crate) struct Handle {
    stop: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<anyhow::Result<()>>>,
    pub(crate) snapshot: watch::Receiver<Snapshot>,
}

impl Handle {
    pub(crate) async fn shutdown(self) -> anyhow::Result<()> {
        self.shutdown_with_budget(Duration::from_secs(5)).await
    }

    async fn shutdown_with_budget(mut self, budget: Duration) -> anyhow::Result<()> {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(task) = self.task.as_mut() {
            match tokio::time::timeout(budget, &mut *task).await {
                Ok(result) => result??,
                Err(_) => {
                    task.abort();
                    let _ = task.await;
                    anyhow::bail!(
                        "membership shutdown budget exhausted; in-flight storage may still finish without publication"
                    );
                }
            }
        }
        Ok(())
    }
}

impl Drop for Handle {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

#[cfg(test)]
pub(crate) async fn start(
    local: String,
    endpoint: Option<String>,
    node: Arc<Node>,
    store: Arc<Store>,
    incoming: mpsc::Receiver<ControlIncoming>,
) -> anyhow::Result<Handle> {
    start_with_metrics(
        local,
        endpoint,
        node,
        store,
        incoming,
        Arc::new(RuntimeMetrics::default()),
    )
    .await
}

pub(crate) async fn start_with_metrics(
    local: String,
    endpoint: Option<String>,
    node: Arc<Node>,
    store: Arc<Store>,
    incoming: mpsc::Receiver<ControlIncoming>,
    metrics: Arc<RuntimeMetrics>,
) -> anyhow::Result<Handle> {
    let persisted_store = Arc::clone(&store);
    let (core, actions) = tokio::task::spawn_blocking(move || {
        let timing = Timing {
            interval: Duration::from_secs(2),
            direct: RPC_BUDGET,
            suspicion_min: Duration::from_secs(5),
            suspicion_max: Duration::from_secs(15),
            confirmations: 3,
            health_max: 8,
        };
        let mut core = Membership::new(local, CAPACITY, timing, 1, 3).map_err(core_error)?;
        let actions = MembershipStore::new(&persisted_store).start(&mut core)?;
        Ok::<_, anyhow::Error>((core, actions))
    })
    .await??;
    let initial = actions
        .into_iter()
        .find_map(|action| {
            if let Action::Publish(update) = action {
                Some(update)
            } else {
                None
            }
        })
        .ok_or_else(|| anyhow::anyhow!("membership startup did not publish a durable identity"))?;
    let owner = record(initial, endpoint, None);
    let (stop, stopped) = oneshot::channel();
    let (snapshot_tx, snapshot) = watch::channel(Snapshot::default());
    let (dispatch_tx, dispatch_rx) = mpsc::channel(64);
    let mut seed = [0u8; 8];
    getrandom::fill(&mut seed)
        .map_err(|error| anyhow::anyhow!("membership random seed: {error}"))?;
    let actor = Actor {
        local_declarations: BTreeMap::new(),
        metrics,
        core,
        owner,
        node,
        store,
        incoming,
        records: BTreeMap::new(),
        timings: BTreeMap::new(),
        sessions: BTreeMap::new(),
        serial: 0,
        now: Duration::ZERO,
        last: Instant::now(),
        jobs: JoinSet::new(),
        pending: BTreeMap::new(),
        cursor: 0,
        snapshot_tx,
        dispatch_tx,
        dispatch_rx,
        rng: u64::from_le_bytes(seed),
        next_exchange: Duration::ZERO,
        hints: BTreeMap::new(),
        pages: BTreeMap::new(),
        successful_probes: 0,
        failed_probes: 0,
        #[cfg(test)]
        tick_lateness_micros: VecDeque::new(),
        writes: JoinSet::new(),
        write_queue: VecDeque::new(),
    };
    let task = tokio::spawn(actor.run(stopped));
    Ok(Handle {
        stop: Some(stop),
        task: Some(task),
        snapshot,
    })
}

fn core_error(error: Error) -> anyhow::Error {
    anyhow::anyhow!("membership: {error:?}")
}

fn version(version: MembershipVersion) -> Version {
    Version {
        epoch: version.epoch,
        incarnation: version.incarnation,
    }
}
fn wire_version(version: Version) -> MembershipVersion {
    MembershipVersion {
        epoch: version.epoch,
        incarnation: version.incarnation,
    }
}

fn record(update: Update, endpoint: Option<String>, reporter: Option<String>) -> MembershipRecord {
    MembershipRecord {
        reporter: reporter.unwrap_or_else(|| update.member.clone()),
        member: update.member,
        version: wire_version(update.version),
        state: wire_state(update.status),
        endpoint,
    }
}

fn wire_state(status: Status) -> MembershipState {
    match status {
        Status::Alive => MembershipState::Alive,
        Status::Suspect => MembershipState::Suspect,
        Status::Dead => MembershipState::Dead,
        Status::Left => MembershipState::Left,
    }
}

struct Completion {
    endpoint: String,
    expected: Option<String>,
    mode: Job,
    timing: Option<nx_net::ControlTiming>,
    result: nx_net::NetResult<(nx_sync::NodeId, ControlReply)>,
}

enum Job {
    Exchange,
    Probe {
        attempt: u64,
        target: Peer,
        route: Peer,
    },
    Helper {
        event: ControlIncoming,
        target: Peer,
    },
}

struct Dispatch {
    indirect: bool,
    attempt: u64,
    route: Peer,
}

struct Actor {
    local_declarations: BTreeMap<String, Version>,
    metrics: Arc<RuntimeMetrics>,
    core: Membership,
    owner: MembershipRecord,
    node: Arc<Node>,
    store: Arc<Store>,
    incoming: mpsc::Receiver<ControlIncoming>,
    records: BTreeMap<String, MembershipRecord>,
    timings: BTreeMap<String, ProbeTiming>,
    sessions: BTreeMap<String, (u64, u64)>,
    serial: u64,
    now: Duration,
    last: Instant,
    jobs: JoinSet<Completion>,
    pending: BTreeMap<String, ()>,
    cursor: usize,
    snapshot_tx: watch::Sender<Snapshot>,
    dispatch_tx: mpsc::Sender<Dispatch>,
    dispatch_rx: mpsc::Receiver<Dispatch>,
    rng: u64,
    next_exchange: Duration,
    hints: BTreeMap<String, String>,
    pages: BTreeMap<String, Option<String>>,
    successful_probes: u64,
    failed_probes: u64,
    #[cfg(test)]
    tick_lateness_micros: VecDeque<u64>,
    writes: JoinSet<(u64, Result<(), super::storage::StorageError>)>,
    write_queue: VecDeque<Action>,
}

impl Actor {
    fn core(&mut self) -> &mut Membership {
        &mut self.core
    }

    fn serial(&mut self) -> anyhow::Result<u64> {
        self.serial = self
            .serial
            .checked_add(1)
            .ok_or_else(|| core_error(Error::Overflow))?;
        Ok(self.serial)
    }

    async fn run(mut self, mut stopped: oneshot::Receiver<()>) -> anyhow::Result<()> {
        let result = self.run_inner(&mut stopped).await;
        if let Err(error) = &result {
            self.core.fence();
            self.snapshot_tx
                .send_modify(|snapshot| snapshot.error = Some(error.to_string()));
            tracing::error!(%error, "membership service fenced");
        }
        self.jobs.abort_all();
        while self.jobs.join_next().await.is_some() {}
        result
    }

    async fn run_inner(&mut self, stopped: &mut oneshot::Receiver<()>) -> anyhow::Result<()> {
        let mut ticker = tokio::time::interval(TICK);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        self.publish_snapshot();
        loop {
            tokio::select! {
                biased;
                _ = &mut *stopped => {
                    let actions = self.core().leave().map_err(core_error)?;
                    self.actions(actions).await?;
                    while let Some(result) = self.writes.join_next().await {
                        self.written(result?).await?;
                    }
                    self.jobs.abort_all();
                    while self.jobs.join_next().await.is_some() {}
                    let endpoints: Vec<_> = self.records.values().filter_map(|record| record.endpoint.clone()).collect();
                    let request = ControlRequest { request_id: self.serial()?, owner: self.owner.clone(), updates: vec![], kind: ControlKind::Exchange { after: None } };
                    let leave = async {
                        for batch in endpoints.chunks(8) {
                            let mut sends = JoinSet::new();
                            for endpoint in batch {
                                let node = Arc::clone(&self.node); let endpoint = endpoint.clone(); let request = request.clone();
                                sends.spawn(async move { let _ = node.control_request(&endpoint, request, RPC_BUDGET).await; });
                            }
                            while sends.join_next().await.is_some() {}
                        }
                    };
                    let _ = tokio::time::timeout(Duration::from_secs(2), leave).await;
                    return Ok(());
                }
                dispatched = self.dispatch_rx.recv() => {
                    if let Some(dispatched) = dispatched {
                        self.metrics.peers.increment(if dispatched.indirect { Metric::ProbeIndirect } else { Metric::ProbeDirect });
                        self.advance();
                        let now = self.now;
                        let actions = self.core().probe_dispatched(dispatched.attempt, &dispatched.route, now).map_err(core_error)?;
                        self.actions(actions).await?;
                    }
                }
                written = self.writes.join_next(), if !self.writes.is_empty() => {
                    self.advance();
                    self.written(written.ok_or_else(|| anyhow::anyhow!("membership write missing"))??).await?;
                }
                result = self.jobs.join_next(), if !self.jobs.is_empty() => {
                    self.advance();
                    let completed = result.ok_or_else(|| anyhow::anyhow!("membership job missing"))??;
                    self.pending.remove(&completed.endpoint);
                    self.completed(completed).await?;
                }
                _tick = ticker.tick() => {
                    #[cfg(test)]
                    self.record_tick_lateness(Instant::now().saturating_duration_since(_tick.into_std()));
                    self.advance();
                    let now = self.now;
                    let actions = self.core().tick(now).map_err(core_error)?;
                    for action in &actions {
                        if let Action::PersistFloor { update, .. } = action
                            && update.status == Status::Dead {
                            self.local_declarations.insert(update.member.clone(), update.version);
                        }
                    }
                    self.actions(actions).await?;
                    let mut rng = self.rng;
                    let actions = self.core().probe_next(now, |upper| {
                        rng = rng.wrapping_add(0x9e3779b97f4a7c15);
                        let mut value = rng;
                        value = (value ^ (value >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
                        value = (value ^ (value >> 27)).wrapping_mul(0x94d049bb133111eb);
                        ((value ^ (value >> 31)) % upper as u64) as usize
                    });
                    self.rng = rng;
                    if let Ok(actions) = actions { self.actions(actions).await?; }
                    self.exchange_next().await?;
                }
                event = self.incoming.recv() => {
                    let Some(event) = event else { anyhow::bail!("membership ingress closed"); };
                    self.advance();
                    self.receive(event).await?;
                }
            }
            self.publish_snapshot();
        }
    }

    fn advance(&mut self) {
        let elapsed = self.last.elapsed();
        self.last = Instant::now();
        if elapsed > TICK * 4 {
            self.core().local_degradation();
        } else {
            self.now = self.now.saturating_add(elapsed);
        }
    }

    #[cfg(test)]
    fn record_tick_lateness(&mut self, lateness: Duration) {
        if self.tick_lateness_micros.len() == SCHEDULING_SAMPLES_LIMIT {
            self.tick_lateness_micros.pop_front();
        }
        self.tick_lateness_micros
            .push_back(lateness.as_micros().min(u64::MAX as u128) as u64);
    }

    fn publish_snapshot(&self) {
        self.metrics
            .peers
            .set(Gauge::ControlIngress, self.incoming.len() as u64);
        self.metrics
            .peers
            .set(Gauge::ControlCapacity, self.incoming.max_capacity() as u64);
        self.metrics
            .peers
            .set(Gauge::MembershipWrites, self.write_queue.len() as u64);
        self.metrics.peers.set(Gauge::MembershipWriteCapacity, 64);
        let mut records: Vec<_> = self.records.values().cloned().collect();
        records.push(self.owner.clone());
        self.snapshot_tx.send_replace(Snapshot {
            probe_timings: self
                .timings
                .values()
                .filter(|sample| {
                    self.records.get(&sample.member).is_some_and(|record| {
                        record.version == sample.version
                            && matches!(
                                record.state,
                                MembershipState::Alive | MembershipState::Suspect
                            )
                    })
                })
                .cloned()
                .collect(),
            records,
            health: self.core.health(),
            error: None,
            successful_probes: self.successful_probes,
            failed_probes: self.failed_probes,
            #[cfg(test)]
            tick_lateness_micros: self.tick_lateness_micros.iter().copied().collect(),
        });
    }

    async fn accept_owner(
        &mut self,
        from: &str,
        owner: MembershipRecord,
    ) -> anyhow::Result<Option<u64>> {
        if owner.member != from || owner.reporter != from || from == self.owner.member {
            return Ok(None);
        }
        let current = self.sessions.get(from).copied();
        if current.is_some_and(|(epoch, _)| owner.version.epoch < epoch) {
            return Ok(None);
        }
        let session = match current {
            Some((epoch, session)) if epoch == owner.version.epoch => session,
            _ => {
                if current.is_none() && self.sessions.len() >= CAPACITY {
                    tracing::warn!("membership session capacity exhausted; new owner rejected");
                    return Ok(None);
                }
                self.serial()?
            }
        };
        let now = self.now;
        if owner.state == MembershipState::Left
            && current.is_some_and(|(epoch, _)| epoch == owner.version.epoch)
        {
            if let Ok(actions) = self
                .core()
                .owner_leave(from, session, version(owner.version), now)
            {
                self.actions(actions).await?;
            }
            return Ok(None);
        }
        if !matches!(owner.state, MembershipState::Alive | MembershipState::Left) {
            return Ok(None);
        }
        let admitted = if current.is_some_and(|(epoch, _)| epoch == owner.version.epoch) {
            self.core()
                .owner_alive(from, session, version(owner.version), now)
        } else {
            self.core()
                .admit(from, version(owner.version), session, now)
        };
        let actions = match admitted {
            Ok(actions) => actions,
            Err(Error::Capacity) => {
                tracing::warn!("membership capacity exhausted; owner rejected");
                return Ok(None);
            }
            Err(_) => return Ok(None),
        };
        if !self
            .core
            .accepted_owner(from, version(owner.version), session)
        {
            return Ok(None);
        }
        self.sessions
            .insert(from.into(), (owner.version.epoch, session));
        self.actions(actions).await?;
        if owner.state == MembershipState::Left {
            let actions = self
                .core()
                .owner_leave(from, session, version(owner.version), now)
                .map_err(core_error)?;
            self.actions(actions).await?;
            return Ok(None);
        } else {
            self.records.insert(from.into(), owner);
        }
        Ok(Some(session))
    }

    async fn actions(&mut self, actions: Vec<Action>) -> anyhow::Result<()> {
        let mut actions = VecDeque::from(actions);
        while let Some(action) = actions.pop_front() {
            match action {
                Action::PersistFloor { .. } => {
                    if self.write_queue.len() >= 64 {
                        anyhow::bail!("membership persistence queue exhausted");
                    }
                    self.write_queue.push_back(action);
                    self.start_write()?;
                }
                Action::Publish(update) => {
                    let previous = if update.member == self.owner.member {
                        Some(&self.owner)
                    } else {
                        self.records.get(&update.member)
                    };
                    if previous.is_none_or(|record| {
                        record.state != wire_state(update.status)
                            || record.version != wire_version(update.version)
                    }) {
                        match update.status {
                            Status::Alive
                                if previous.is_some_and(|record| {
                                    record.state == MembershipState::Suspect
                                }) =>
                            {
                                self.metrics.peers.increment(Metric::Refuted)
                            }
                            Status::Dead => self.metrics.peers.increment(
                                if self.local_declarations.remove(&update.member)
                                    == Some(update.version)
                                {
                                    Metric::DeclaredDead
                                } else {
                                    Metric::LearnedDead
                                },
                            ),
                            Status::Left => self.metrics.peers.increment(Metric::Left),
                            _ => {}
                        }
                    }
                    if update.member == self.owner.member {
                        self.owner = record(update, self.owner.endpoint.clone(), None);
                    } else {
                        let endpoint = self
                            .records
                            .get(&update.member)
                            .and_then(|record| record.endpoint.clone());
                        let reporter = if matches!(update.status, Status::Alive | Status::Left) {
                            update.member.clone()
                        } else {
                            self.owner.member.clone()
                        };
                        self.records.insert(
                            update.member.clone(),
                            record(update, endpoint, Some(reporter)),
                        );
                    }
                }
                Action::Suspicion { update, reporter } => {
                    if self.records.get(&update.member).is_none_or(|record| {
                        record.state != MembershipState::Suspect
                            || record.version != wire_version(update.version)
                    }) {
                        self.metrics.peers.increment(Metric::Suspect);
                    }
                    let endpoint = self
                        .records
                        .get(&update.member)
                        .and_then(|record| record.endpoint.clone());
                    self.records.insert(
                        update.member.clone(),
                        record(update, endpoint, Some(reporter)),
                    );
                }
                Action::Buddy { member, .. } => {
                    if let Some(endpoint) = self
                        .records
                        .get(&member)
                        .and_then(|record| record.endpoint.clone())
                    {
                        self.launch(
                            endpoint,
                            Some(member),
                            ControlKind::Exchange { after: None },
                            Job::Exchange,
                        )?;
                    }
                }
                Action::Probe(probe::Action::Direct {
                    attempt, target, ..
                }) => {
                    if let Some(endpoint) = self
                        .records
                        .get(&target.id)
                        .and_then(|record| record.endpoint.clone())
                    {
                        if !self.launch(
                            endpoint,
                            Some(target.id.clone()),
                            ControlKind::Probe {
                                target: target.id.clone(),
                                version: wire_version(target.version),
                            },
                            Job::Probe {
                                attempt,
                                target: target.clone(),
                                route: target,
                            },
                        )? {
                            let now = self.now;
                            actions.extend(
                                self.core()
                                    .probe_send_failed(attempt, now)
                                    .map_err(core_error)?,
                            );
                        }
                    } else {
                        let now = self.now;
                        actions.extend(
                            self.core()
                                .probe_send_failed(attempt, now)
                                .map_err(core_error)?,
                        );
                    }
                }
                Action::Probe(probe::Action::Indirect {
                    attempt,
                    helper,
                    target,
                    ..
                }) => {
                    if let Some(endpoint) = self
                        .records
                        .get(&helper.id)
                        .and_then(|record| record.endpoint.clone())
                    {
                        if !self.launch(
                            endpoint,
                            Some(helper.id.clone()),
                            ControlKind::Indirect {
                                target: target.id.clone(),
                                version: wire_version(target.version),
                            },
                            Job::Probe {
                                attempt,
                                target,
                                route: helper,
                            },
                        )? {
                            let now = self.now;
                            actions.extend(
                                self.core()
                                    .probe_send_failed(attempt, now)
                                    .map_err(core_error)?,
                            );
                        }
                    } else {
                        let now = self.now;
                        actions.extend(
                            self.core()
                                .probe_send_failed(attempt, now)
                                .map_err(core_error)?,
                        );
                    }
                }
                Action::Probe(probe::Action::Finished { outcome, .. }) => match outcome {
                    probe::Outcome::Success => {
                        self.metrics.peers.increment(Metric::ProbeSuccess);
                        self.successful_probes = self.successful_probes.saturating_add(1)
                    }
                    probe::Outcome::Failed => {
                        self.metrics.peers.increment(Metric::ProbeFailed);
                        self.failed_probes = self.failed_probes.saturating_add(1)
                    }
                    probe::Outcome::LocalDegradation => {
                        self.metrics.peers.increment(Metric::ProbeDegraded)
                    }
                },
                _ => {}
            }
        }
        Ok(())
    }

    async fn receive(&mut self, event: ControlIncoming) -> anyhow::Result<()> {
        let from = event.from.to_string();
        let accepted_session = self
            .accept_owner(&from, event.request.owner.clone())
            .await?;
        if let Some(session) = accepted_session {
            self.updates(&from, session, &event.request.updates).await?;
        }
        if accepted_session.is_some()
            && let ControlKind::Indirect { target, version } = &event.request.kind
            && let Some(record) = self.records.get(target)
            && record.version == *version
            && let Some(endpoint) = &record.endpoint
            && let Some((_, session)) = self.sessions.get(target)
        {
            let target = Peer {
                id: target.clone(),
                version: super::runtime::version(*version),
                session: *session,
            };
            let endpoint = endpoint.clone();
            if self.jobs.len() < 8 && self.sessions.contains_key(&from) {
                let kind = ControlKind::Probe {
                    target: target.id.clone(),
                    version: wire_version(target.version),
                };
                self.launch(
                    endpoint,
                    Some(target.id.clone()),
                    kind,
                    Job::Helper { event, target },
                )?;
                return Ok(());
            }
        }
        let ack = match &event.request.kind {
            ControlKind::Probe { target, version } => {
                target == &self.owner.member && version == &self.owner.version
            }
            _ => false,
        };
        let after = match &event.request.kind {
            ControlKind::Exchange { after } => after.as_deref(),
            _ => None,
        };
        let mut page = self
            .records
            .values()
            .filter(|record| after.is_none_or(|after| record.member.as_str() > after));
        let records: Vec<_> = page
            .by_ref()
            .take(if accepted_session.is_some() {
                nx_net::MAX_CONTROL_RECORDS
            } else {
                0
            })
            .cloned()
            .collect();
        let next = page
            .next()
            .and_then(|_| records.last().map(|record| record.member.clone()));
        let _ = event.respond_to.send(ControlReply {
            request_id: event.request.request_id,
            owner: self.owner.clone(),
            ack,
            nack: false,
            records,
            next,
        });
        Ok(())
    }

    fn start_write(&mut self) -> anyhow::Result<()> {
        if !self.writes.is_empty() {
            return Ok(());
        }
        while let Some(action) = self.write_queue.pop_front() {
            if !self.core.pending_floor(&action) {
                continue;
            }
            let Action::PersistFloor { token, update } = action else {
                continue;
            };
            let store = Arc::clone(&self.store);
            let local = self.core.local_id().to_string();
            let epoch = self
                .core
                .local_epoch()
                .ok_or_else(|| core_error(Error::NotReady))?;
            self.writes.spawn_blocking(move || {
                (
                    token,
                    MembershipStore::new(&store).persist_update(&local, epoch, &update),
                )
            });
            break;
        }
        Ok(())
    }

    async fn written(
        &mut self,
        (token, result): (u64, Result<(), super::storage::StorageError>),
    ) -> anyhow::Result<()> {
        if let Err(error) = result {
            let _ = self.core.confirm_floor(token, false);
            self.core.fence();
            return Err(error.into());
        }
        let actions = self.core.confirm_floor(token, true).map_err(core_error)?;
        self.actions(actions).await?;
        self.start_write()?;
        Ok(())
    }

    async fn exchange_next(&mut self) -> anyhow::Result<()> {
        if self.jobs.len() >= 6 || self.now < self.next_exchange {
            return Ok(());
        }
        self.next_exchange = self.now.saturating_add(Duration::from_millis(250));
        let mut endpoints: Vec<_> = self
            .records
            .values()
            .filter_map(|record| {
                record
                    .endpoint
                    .clone()
                    .map(|endpoint| (endpoint, record.member.clone()))
            })
            .collect();
        endpoints.extend(
            self.hints
                .iter()
                .map(|(member, endpoint)| (endpoint.clone(), member.clone())),
        );
        for (addr, member) in self.node.connected_peers().await {
            if let Some(info) = self.node.connection_info(&addr).await
                && let Some(endpoint) = info.dialed_endpoint
            {
                endpoints.push((endpoint, member.to_string()));
            }
        }
        endpoints.sort();
        endpoints.dedup();
        if endpoints.is_empty() {
            return Ok(());
        }
        self.cursor %= endpoints.len();
        let (endpoint, expected) = endpoints[self.cursor].clone();
        self.cursor += 1;
        if self.pending.contains_key(&endpoint) {
            return Ok(());
        }
        let after = self.pages.get(&expected).cloned().flatten();
        self.launch(
            endpoint,
            Some(expected),
            ControlKind::Exchange { after },
            Job::Exchange,
        )?;
        Ok(())
    }

    fn launch(
        &mut self,
        endpoint: String,
        expected: Option<String>,
        kind: ControlKind,
        mode: Job,
    ) -> anyhow::Result<bool> {
        if self.jobs.len() >= 8 {
            return Ok(false);
        }
        let mut updates = Vec::new();
        if let Some(target) = expected
            .as_ref()
            .and_then(|expected| self.records.get(expected))
            && target.reporter == self.owner.member
        {
            updates.push(target.clone());
        }
        updates.extend(
            self.records
                .values()
                .filter(|record| {
                    record.reporter == self.owner.member
                        && expected
                            .as_ref()
                            .is_none_or(|expected| expected != &record.member)
                })
                .take(nx_net::MAX_CONTROL_RECORDS - updates.len())
                .cloned(),
        );
        let request = ControlRequest {
            request_id: self.serial()?,
            owner: self.owner.clone(),
            updates,
            kind,
        };
        if matches!(mode, Job::Exchange) {
            self.pending.insert(endpoint.clone(), ());
        }
        let node = Arc::clone(&self.node);
        let dispatch_tx = self.dispatch_tx.clone();
        let budget = match &mode {
            Job::Probe { target, route, .. } if target != route => Duration::from_secs(1),
            Job::Probe { .. } => RPC_BUDGET * (self.core().health() + 1),
            _ => RPC_BUDGET,
        };
        self.jobs.spawn(async move {
            let (sent, received) = oneshot::channel();
            let query = node.control_request_timed_tracked(&endpoint, request, budget, Some(sent));
            tokio::pin!(query);
            let result = tokio::select! {
                biased;
                sent = received => {
                    if sent.is_ok() && let Job::Probe { attempt, route, .. } = &mode
                        && dispatch_tx.try_send(Dispatch { indirect: matches!(&mode, Job::Probe { target, route, .. } if target != route), attempt: *attempt, route: route.clone() }).is_err() {
                            return Completion { endpoint: endpoint.clone(), expected, mode, timing: None, result: Err(nx_net::NetError::PeerLimitReached(64)) };
                    }
                    query.await
                }
                result = &mut query => result,
            };
            let (result, timing) = match result {
                Ok((peer, reply, timing)) => (Ok((peer, reply)), Some(timing)),
                Err(error) => (Err(error), None),
            };
            Completion { endpoint: endpoint.clone(), expected, mode, timing, result }
        });
        Ok(true)
    }

    async fn completed(&mut self, completion: Completion) -> anyhow::Result<()> {
        let result = completion.result.and_then(|(peer, reply)| {
            if completion
                .expected
                .as_ref()
                .is_some_and(|expected| expected != &peer.to_string())
            {
                Err(nx_net::NetError::InvalidMessage(
                    "unexpected control identity".into(),
                ))
            } else {
                Ok((peer, reply))
            }
        });
        let mut accepted_session = None;
        let result = match result {
            Ok((peer, reply)) => {
                accepted_session = self
                    .accept_owner(&peer.to_string(), reply.owner.clone())
                    .await?;
                if accepted_session.is_some() {
                    Ok((peer, reply))
                } else {
                    Err(nx_net::NetError::InvalidMessage(
                        "unaccepted control owner".into(),
                    ))
                }
            }
            Err(error) => Err(error),
        };
        let now = self.now;
        if let Job::Probe {
            attempt,
            target,
            route,
        } = &completion.mode
        {
            let actions = match &result {
                Ok((_, reply)) if reply.ack || reply.nack => self
                    .core()
                    .probe_reply(*attempt, target, route, reply.ack, now),
                Ok(_) => self.core().probe_send_failed(*attempt, now),
                Err(
                    nx_net::NetError::PeerLimitReached(_)
                    | nx_net::NetError::TlsError(_)
                    | nx_net::NetError::InvalidMessage(_),
                ) => self.core().probe_send_failed(*attempt, now),
                Err(nx_net::NetError::Wire(nx_net::WireError::RateLimited { .. })) => {
                    self.core().probe_send_failed(*attempt, now)
                }
                _ => Ok(vec![]),
            }
            .map_err(core_error)?;
            let direct_success = target == route
                && actions.iter().any(|action| {
                    matches!(action, Action::Probe(probe::Action::Finished {
                    attempt: completed, outcome: probe::Outcome::Success, ..
                }) if completed == attempt)
                });
            if direct_success
                && let (Ok((peer, reply)), Some(timing)) = (&result, completion.timing)
                && reply.ack
                && version(reply.owner.version) == target.version
                && accepted_session == Some(target.session)
            {
                self.timings.insert(
                    peer.to_string(),
                    ProbeTiming {
                        member: peer.to_string(),
                        version: reply.owner.version,
                        timing,
                    },
                );
            }
            self.actions(actions).await?;
        }
        if let Job::Helper { event, target } = completion.mode {
            let ack = result.as_ref().is_ok_and(|(peer, reply)| {
                peer.to_string() == target.id
                    && reply.ack
                    && version(reply.owner.version) == target.version
            });
            let nack = !ack
                && matches!(
                    result,
                    Err(nx_net::NetError::Io(_)
                        | nx_net::NetError::Timeout
                        | nx_net::NetError::ConnectionFailed(_))
                );
            let _ = event.respond_to.send(ControlReply {
                request_id: event.request.request_id,
                owner: self.owner.clone(),
                ack,
                nack,
                records: vec![],
                next: None,
            });
        }
        if let Ok((peer, reply)) = result
            && let Some(session) = accepted_session
        {
            let peer = peer.to_string();
            self.hints.remove(&peer);
            if self.pages.len() < CAPACITY || self.pages.contains_key(&peer) {
                self.pages.insert(peer.clone(), reply.next);
            }
            self.updates(&peer, session, &reply.records).await?;
        }
        Ok(())
    }

    async fn updates(
        &mut self,
        from: &str,
        session: u64,
        records: &[MembershipRecord],
    ) -> anyhow::Result<()> {
        for record in records {
            if record.state == MembershipState::Left {
                let now = self.now;
                if let Ok(actions) = self.core().relay_leave(
                    from,
                    session,
                    &record.member,
                    version(record.version),
                    now,
                ) {
                    self.actions(actions).await?;
                }
                continue;
            }
            if matches!(record.state, MembershipState::Alive | MembershipState::Left)
                && record.member != self.owner.member
            {
                if let Some(endpoint) = &record.endpoint
                    && self.hints.len() < 64
                    && !self.records.contains_key(&record.member)
                {
                    self.hints.insert(record.member.clone(), endpoint.clone());
                }
                continue;
            }
            if record.reporter != from {
                continue;
            }
            let now = self.now;
            let actions = match record.state {
                MembershipState::Suspect => {
                    self.core()
                        .suspect(from, session, &record.member, version(record.version), now)
                }
                MembershipState::Dead => self.core().report_dead(
                    from,
                    session,
                    &record.member,
                    version(record.version),
                    now,
                ),
                _ => Ok(vec![]),
            };
            if let Ok(actions) = actions {
                self.actions(actions).await?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn owner(member: &str, epoch: u64, incarnation: u64) -> MembershipRecord {
        MembershipRecord {
            member: member.into(),
            reporter: member.into(),
            version: MembershipVersion { epoch, incarnation },
            state: MembershipState::Alive,
            endpoint: None,
        }
    }

    fn actor(floors: &[super::super::schema::Floor]) -> (tempfile::TempDir, Actor) {
        let directory = tempfile::tempdir().unwrap();
        let store = Arc::new(Store::open(directory.path()).unwrap());
        let timing = super::super::suspicion::tests::timing();
        let mut initial = Membership::new("local".into(), CAPACITY, timing, 1, 3).unwrap();
        MembershipStore::new(&store).start(&mut initial).unwrap();
        for floor in floors {
            store
                .set(
                    format!("__nx/membership/floor/{}", floor.member).as_bytes(),
                    &floor.encode().unwrap(),
                )
                .unwrap();
        }
        store.flush().unwrap();
        let mut core = Membership::new("local".into(), CAPACITY, timing, 1, 3).unwrap();
        MembershipStore::new(&store).start(&mut core).unwrap();
        let mut node = Node::new(nx_net::NodeConfig::new(
            nx_sync::NodeId::new("local"),
            "127.0.0.1:0",
        ));
        let incoming = node.take_control_receiver().unwrap();
        let (snapshot_tx, _) = watch::channel(Snapshot::default());
        let (dispatch_tx, dispatch_rx) = mpsc::channel(64);
        (
            directory,
            Actor {
                local_declarations: BTreeMap::new(),
                metrics: Arc::new(RuntimeMetrics::default()),
                core,
                owner: owner("local", 2, 0),
                node: Arc::new(node),
                store,
                incoming,
                records: BTreeMap::new(),
                timings: BTreeMap::new(),
                sessions: BTreeMap::new(),
                serial: 0,
                now: Duration::ZERO,
                last: Instant::now(),
                jobs: JoinSet::new(),
                pending: BTreeMap::new(),
                cursor: 0,
                snapshot_tx,
                dispatch_tx,
                dispatch_rx,
                rng: 1,
                next_exchange: Duration::ZERO,
                hints: BTreeMap::new(),
                pages: BTreeMap::new(),
                successful_probes: 0,
                failed_probes: 0,
                tick_lateness_micros: VecDeque::new(),
                writes: JoinSet::new(),
                write_queue: VecDeque::new(),
            },
        )
    }

    #[tokio::test]
    async fn only_current_successful_direct_probes_publish_round_trip_samples() {
        for case in 0..8 {
            let (_directory, mut actor) = actor(&[]);
            for member in ["bob", "carol"] {
                actor
                    .accept_owner(member, owner(member, 1, 0))
                    .await
                    .unwrap();
            }
            let actions = actor.core.probe_next(Duration::ZERO, |_| 0).unwrap();
            let (attempt, target) = actions
                .into_iter()
                .find_map(|action| match action {
                    Action::Probe(probe::Action::Direct {
                        attempt, target, ..
                    }) => Some((attempt, target)),
                    _ => None,
                })
                .unwrap();
            actor
                .core
                .probe_dispatched(attempt, &target, Duration::ZERO)
                .unwrap();
            let mut route = target.clone();
            if case == 5 {
                let helper = if target.id == "bob" { "carol" } else { "bob" };
                route = Peer {
                    id: helper.into(),
                    version: Version {
                        epoch: 1,
                        incarnation: 0,
                    },
                    session: actor.sessions[helper].1,
                };
            }
            if case == 7 {
                route.session += 1;
            }
            if case == 4 {
                actor
                    .core
                    .probe_send_failed(attempt, Duration::ZERO)
                    .unwrap();
            }
            let mut response_owner = owner(&route.id, 1, 0);
            if case == 6 {
                response_owner.version.incarnation += 1;
            }
            let timing = nx_net::ControlTiming {
                started_at: Instant::now(),
                observed_at: Instant::now(),
                round_trip: Duration::from_millis(10),
            };
            let response = if case == 1 {
                Err(nx_net::NetError::Timeout)
            } else {
                Ok((
                    nx_sync::NodeId::new(if case == 2 { "stranger" } else { &route.id }),
                    ControlReply {
                        request_id: 1,
                        owner: response_owner,
                        ack: case != 3,
                        nack: case == 3,
                        records: vec![],
                        next: None,
                    },
                ))
            };
            actor
                .completed(Completion {
                    endpoint: "127.0.0.1:9000".into(),
                    expected: Some(route.id.clone()),
                    mode: Job::Probe {
                        attempt,
                        target: target.clone(),
                        route,
                    },
                    timing: Some(timing),
                    result: response,
                })
                .await
                .unwrap();
            assert_eq!(actor.timings.len(), usize::from(case == 0), "case {case}");
            if case == 0 {
                let accepted = actor.timings[&target.id].timing.observed_at;
                actor
                    .completed(Completion {
                        endpoint: "127.0.0.1:9000".into(),
                        expected: Some(target.id.clone()),
                        mode: Job::Probe {
                            attempt,
                            target: target.clone(),
                            route: target.clone(),
                        },
                        timing: Some(nx_net::ControlTiming {
                            observed_at: accepted + Duration::from_secs(1),
                            ..timing
                        }),
                        result: Ok((
                            nx_sync::NodeId::new(&target.id),
                            ControlReply {
                                request_id: 2,
                                owner: owner(&target.id, 1, 0),
                                ack: true,
                                nack: false,
                                records: vec![],
                                next: None,
                            },
                        )),
                    })
                    .await
                    .unwrap();
                assert_eq!(actor.timings[&target.id].timing.observed_at, accepted);
            }
        }
    }

    #[tokio::test]
    async fn owner_admission_after_restore_does_not_cache_rejected_sessions() {
        for status in [Status::Dead, Status::Left] {
            let floor = super::super::schema::Floor {
                member: "bob".into(),
                version: Version {
                    epoch: 7,
                    incarnation: 2,
                },
                status,
                closed_epoch: (status == Status::Left).then_some(7),
            };
            let (_directory, mut actor) = actor(&[floor]);
            assert!(
                actor
                    .accept_owner("bob", owner("bob", 7, 2))
                    .await
                    .unwrap()
                    .is_none()
            );
            assert!(!actor.sessions.contains_key("bob"));
            let refutation = actor.accept_owner("bob", owner("bob", 7, 3)).await.unwrap();
            assert_eq!(refutation.is_some(), status == Status::Dead);
            if status == Status::Left {
                assert!(!actor.sessions.contains_key("bob"));
            }
            let session = actor
                .accept_owner("bob", owner("bob", 8, 0))
                .await
                .unwrap()
                .unwrap();
            assert!(actor.core.accepted_owner(
                "bob",
                Version {
                    epoch: 8,
                    incarnation: 0
                },
                session
            ));
        }
    }

    #[tokio::test]
    async fn stale_owner_requests_and_replies_cannot_borrow_current_session() {
        for state in [
            MembershipState::Suspect,
            MembershipState::Dead,
            MembershipState::Left,
        ] {
            for response in [false, true] {
                let (_directory, mut actor) = actor(&[]);
                actor
                    .accept_owner("bob", owner("bob", 1, 0))
                    .await
                    .unwrap()
                    .unwrap();
                actor
                    .accept_owner("bob", owner("bob", 2, 0))
                    .await
                    .unwrap()
                    .unwrap();
                actor
                    .accept_owner("carol", owner("carol", 1, 0))
                    .await
                    .unwrap()
                    .unwrap();
                let before = actor.core.snapshot();
                let session = actor.sessions["bob"];
                let mut update = owner("carol", 1, 0);
                update.reporter = "bob".into();
                update.state = state;
                if response {
                    actor
                        .completed(Completion {
                            endpoint: "127.0.0.1:9999".into(),
                            expected: Some("bob".into()),
                            mode: Job::Exchange,
                            timing: None,
                            result: Ok((
                                nx_sync::NodeId::new("bob"),
                                ControlReply {
                                    request_id: 42,
                                    owner: owner("bob", 1, 0),
                                    ack: false,
                                    nack: false,
                                    records: vec![update],
                                    next: Some("cursor".into()),
                                },
                            )),
                        })
                        .await
                        .unwrap();
                } else {
                    let (respond_to, reply) = oneshot::channel();
                    actor
                        .receive(ControlIncoming {
                            from: nx_sync::NodeId::new("bob"),
                            verification: nx_net::PeerIdentityVerification::Unverified,
                            request: ControlRequest {
                                request_id: 42,
                                owner: owner("bob", 1, 0),
                                updates: vec![update],
                                kind: ControlKind::Exchange { after: None },
                            },
                            respond_to,
                        })
                        .await
                        .unwrap();
                    assert!(reply.await.unwrap().records.is_empty());
                }
                assert_eq!(actor.core.snapshot(), before);
                assert_eq!(actor.sessions["bob"], session);
                assert!(actor.writes.is_empty());
                assert!(actor.write_queue.is_empty());
                assert!(actor.pages.is_empty());
                assert!(actor.hints.is_empty());
                let mut fresh = owner("carol", 1, 0);
                fresh.reporter = "bob".into();
                fresh.state = state;
                actor.updates("bob", session.1, &[fresh]).await.unwrap();
                if state == MembershipState::Suspect {
                    assert_ne!(actor.core.snapshot(), before);
                } else {
                    assert!(!actor.writes.is_empty());
                }
            }
        }
    }

    #[tokio::test]
    async fn owner_admission_at_capacity_allows_known_member_restart() {
        let (_directory, mut actor) = actor(&[]);
        for index in 0..CAPACITY {
            let member = format!("member-{index}");
            actor
                .accept_owner(&member, owner(&member, 1, 0))
                .await
                .unwrap()
                .unwrap();
        }
        let previous = actor.sessions["member-0"];
        let replacement = actor
            .accept_owner("member-0", owner("member-0", 2, 0))
            .await
            .unwrap()
            .unwrap();
        assert_ne!(replacement, previous.1);
        assert_eq!(actor.sessions.len(), CAPACITY);
        assert!(
            actor
                .accept_owner("extra", owner("extra", 1, 0))
                .await
                .unwrap()
                .is_none()
        );
        assert!(!actor.sessions.contains_key("extra"));
        assert!(actor.core.accepted_owner(
            "member-0",
            Version {
                epoch: 2,
                incarnation: 0
            },
            replacement
        ));
    }

    struct Running {
        _directory: tempfile::TempDir,
        store: Arc<Store>,
        node: Arc<Node>,
        handle: Option<Handle>,
        endpoint: String,
    }

    impl Running {
        async fn new(member: &str, format: nx_net::SerializationFormat) -> Self {
            let directory = tempfile::tempdir().unwrap();
            let store = Arc::new(Store::open(directory.path()).unwrap());
            let (node, incoming, endpoint) = Self::network(member, format).await;
            let handle = start(
                member.into(),
                Some(endpoint.clone()),
                Arc::clone(&node),
                Arc::clone(&store),
                incoming,
            )
            .await
            .unwrap();
            Self {
                _directory: directory,
                store,
                node,
                handle: Some(handle),
                endpoint,
            }
        }

        async fn network(
            member: &str,
            format: nx_net::SerializationFormat,
        ) -> (Arc<Node>, mpsc::Receiver<ControlIncoming>, String) {
            let mut node = Node::new(
                nx_net::NodeConfig::new(nx_sync::NodeId::new(member), "127.0.0.1:0")
                    .with_serialization_format(format),
            );
            let incoming = node.take_control_receiver().unwrap();
            let endpoint = node.start_listener().await.unwrap().to_string();
            (Arc::new(node), incoming, endpoint)
        }

        async fn wait(&mut self, predicate: impl Fn(&Snapshot) -> bool) -> Snapshot {
            let receiver = &mut self.handle.as_mut().unwrap().snapshot;
            tokio::time::timeout(
                Duration::from_secs(30),
                receiver.wait_for(|snapshot| snapshot.error.is_some() || predicate(snapshot)),
            )
            .await
            .unwrap()
            .unwrap()
            .clone()
        }

        async fn owner(&mut self, member: &str) -> MembershipRecord {
            self.wait(|snapshot| {
                snapshot
                    .records
                    .iter()
                    .any(|record| record.member == member)
            })
            .await
            .records
            .into_iter()
            .find(|record| record.member == member)
            .unwrap()
        }

        async fn stop(mut self) {
            if let Some(handle) = self.handle.take() {
                handle.shutdown().await.unwrap();
            }
            self.node.shutdown().await;
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
    async fn production_adapter_probes_helpers_refutes_leaves_and_restarts_both_codecs() {
        for format in [
            nx_net::SerializationFormat::Json,
            nx_net::SerializationFormat::Bincode,
        ] {
            let mut alice = Running::new("alice", format).await;
            let mut bob = Running::new("bob", format).await;
            let mut carol = Running::new("carol", format).await;
            alice.node.connect_to_peer(&bob.endpoint).await.unwrap();
            carol.node.connect_to_peer(&bob.endpoint).await.unwrap();
            for runtime in [&mut alice, &mut bob, &mut carol] {
                let snapshot = runtime
                    .wait(|snapshot| snapshot.records.len() == 3 && snapshot.successful_probes > 0)
                    .await;
                assert!(snapshot.error.is_none());
                assert_eq!(snapshot.failed_probes, 0);
            }
            let owner = alice.owner("alice").await;
            let target = carol.owner("carol").await;
            let reply = alice
                .node
                .control_request(
                    &bob.endpoint,
                    ControlRequest {
                        request_id: 9001,
                        owner: owner.clone(),
                        updates: vec![],
                        kind: ControlKind::Indirect {
                            target: target.member,
                            version: target.version,
                        },
                    },
                    Duration::from_secs(2),
                )
                .await
                .unwrap()
                .1;
            assert!(reply.ack);
            assert!(!reply.nack);

            let mut accused = bob.owner("bob").await;
            accused.state = MembershipState::Suspect;
            accused.reporter = "alice".into();
            let reply = alice
                .node
                .control_request(
                    &bob.endpoint,
                    ControlRequest {
                        request_id: 9002,
                        owner,
                        updates: vec![accused.clone()],
                        kind: ControlKind::Exchange { after: None },
                    },
                    Duration::from_secs(2),
                )
                .await
                .unwrap()
                .1;
            assert_eq!(
                reply.owner.version.incarnation,
                accused.version.incarnation + 1
            );
            alice
                .wait(|snapshot| {
                    snapshot.records.iter().any(|record| {
                        record.member == "bob"
                            && record.version.incarnation == reply.owner.version.incarnation
                    })
                })
                .await;
            bob.handle.take().unwrap().shutdown().await.unwrap();
            bob.node.shutdown().await;
            let snapshot = alice
                .wait(|snapshot| {
                    snapshot.records.iter().any(|record| {
                        record.member == "bob" && record.state == MembershipState::Left
                    })
                })
                .await;
            assert!(snapshot.error.is_none());
            assert!(
                alice
                    .store
                    .get(b"__nx/membership/floor/bob")
                    .unwrap()
                    .is_some()
            );

            let (node, incoming, endpoint) = Running::network("bob", format).await;
            bob.node = node;
            bob.endpoint = endpoint;
            bob.handle = Some(
                start(
                    "bob".into(),
                    Some(bob.endpoint.clone()),
                    Arc::clone(&bob.node),
                    Arc::clone(&bob.store),
                    incoming,
                )
                .await
                .unwrap(),
            );
            let owner = bob.owner("bob").await;
            assert_eq!(owner.version.epoch, 2);
            bob.node
                .control_request(
                    &alice.endpoint,
                    ControlRequest {
                        request_id: 9003,
                        owner,
                        updates: vec![],
                        kind: ControlKind::Exchange { after: None },
                    },
                    Duration::from_secs(2),
                )
                .await
                .unwrap();
            alice
                .wait(|snapshot| {
                    snapshot.records.iter().any(|record| {
                        record.member == "bob"
                            && record.state == MembershipState::Alive
                            && record.version.epoch == 2
                    })
                })
                .await;
            alice.stop().await;
            bob.stop().await;
            carol.stop().await;
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
    async fn production_adapter_serves_probes_during_flush_and_fences_storage_errors() {
        for fail_flush in [false, true] {
            let format = nx_net::SerializationFormat::Bincode;
            let mut alice = Running::new("alice", format).await;
            let mut bob = Running::new("bob", format).await;
            alice.node.connect_to_peer(&bob.endpoint).await.unwrap();
            alice.wait(|snapshot| snapshot.records.len() == 2).await;
            let baseline_tick_samples = alice
                .handle
                .as_ref()
                .unwrap()
                .snapshot
                .borrow()
                .tick_lateness_micros
                .len();
            let target = alice.owner("alice").await;
            let mut owner = bob.owner("bob").await;
            owner.state = MembershipState::Left;
            let (reached_tx, reached_rx) = std::sync::mpsc::channel();
            let (resume_tx, resume_rx) = std::sync::mpsc::channel();
            if fail_flush {
                alice.store.inject_disk_full_on_flush(true);
            } else {
                alice.store.pause_next_flush(reached_tx, resume_rx);
            }
            bob.node
                .control_request(
                    &alice.endpoint,
                    ControlRequest {
                        request_id: 9200,
                        owner: owner.clone(),
                        updates: vec![],
                        kind: ControlKind::Exchange { after: None },
                    },
                    Duration::from_secs(2),
                )
                .await
                .unwrap();
            if !fail_flush {
                tokio::task::spawn_blocking(move || {
                    reached_rx.recv_timeout(Duration::from_secs(5))
                })
                .await
                .unwrap()
                .unwrap();
                let paused_snapshot = alice
                    .wait(|snapshot| {
                        snapshot.tick_lateness_micros.len() >= baseline_tick_samples + 40
                    })
                    .await;
                assert_eq!(paused_snapshot.failed_probes, 0);
                assert!(
                    paused_snapshot
                        .records
                        .iter()
                        .all(|record| { record.state == MembershipState::Alive })
                );
                let mut tick_lateness =
                    paused_snapshot.tick_lateness_micros[baseline_tick_samples..].to_vec();
                tick_lateness.sort_unstable();
                eprintln!(
                    "slow-storage-control {}",
                    serde_json::json!({
                        "tick_samples": tick_lateness.len(),
                        "tick_lateness_p50_us": tick_lateness[(tick_lateness.len() * 50 - 1) / 100],
                        "tick_lateness_p95_us": tick_lateness[(tick_lateness.len() * 95 - 1) / 100],
                        "tick_lateness_max_us": tick_lateness.last(),
                        "successful_probes": paused_snapshot.successful_probes,
                        "failed_probes": paused_snapshot.failed_probes,
                        "health": paused_snapshot.health
                    })
                );
                let reply = bob
                    .node
                    .control_request(
                        &alice.endpoint,
                        ControlRequest {
                            request_id: 9201,
                            owner,
                            updates: vec![],
                            kind: ControlKind::Probe {
                                target: target.member,
                                version: target.version,
                            },
                        },
                        Duration::from_secs(2),
                    )
                    .await
                    .unwrap()
                    .1;
                assert!(reply.ack, "probe stalled behind membership flush");
                assert!(
                    !reply
                        .records
                        .iter()
                        .any(|record| record.member == "bob"
                            && record.state == MembershipState::Left)
                );
                assert!(
                    !alice
                        .handle
                        .as_ref()
                        .unwrap()
                        .snapshot
                        .borrow()
                        .records
                        .iter()
                        .any(|record| record.member == "bob"
                            && record.state == MembershipState::Left)
                );
                resume_tx.send(()).unwrap();
            }
            let snapshot = alice
                .wait(|snapshot| {
                    snapshot.records.iter().any(|record| {
                        record.member == "bob" && record.state == MembershipState::Left
                    })
                })
                .await;
            if fail_flush {
                assert!(
                    snapshot
                        .error
                        .as_ref()
                        .is_some_and(|error| error.contains("membership storage"))
                );
                assert!(
                    !snapshot
                        .records
                        .iter()
                        .any(|record| record.member == "bob"
                            && record.state == MembershipState::Left)
                );
                assert!(alice.handle.take().unwrap().shutdown().await.is_err());
                alice.node.shutdown().await;
            } else {
                assert!(snapshot.error.is_none());
                alice.stop().await;
            }
            bob.stop().await;
        }
    }

    #[tokio::test]
    async fn production_adapter_makes_progress_with_cpu_bound_wasm() {
        let format = nx_net::SerializationFormat::Bincode;
        let mut alice = Running::new("alice", format).await;
        let bob = Running::new("bob", format).await;
        alice.node.connect_to_peer(&bob.endpoint).await.unwrap();
        alice.wait(|snapshot| snapshot.records.len() == 2).await;
        let baseline = alice
            .handle
            .as_ref()
            .unwrap()
            .snapshot
            .borrow()
            .successful_probes;
        let directory = tempfile::tempdir().unwrap();
        let mut guest_runtime = crate::runtime::Runtime::new(crate::runtime::RuntimeConfig {
            datastore_path: directory.path().join("guest"),
            enable_wasi: false,
            ..crate::runtime::RuntimeConfig::default()
        })
        .unwrap();
        let wasm = [
            0, 97, 115, 109, 1, 0, 0, 0, 1, 4, 1, 96, 0, 0, 3, 2, 1, 0, 7, 7, 1, 3, 114, 117, 110,
            0, 0, 10, 9, 1, 7, 0, 3, 64, 12, 0, 11, 11,
        ];
        let started = Instant::now();
        {
            let guest = guest_runtime.run_module(&wasm);
            tokio::pin!(guest);
            let snapshot = tokio::select! {
                biased;
                result = &mut guest => panic!("CPU-bound guest unexpectedly completed: {result:?}"),
                snapshot = alice.wait(|snapshot| {
                    snapshot.successful_probes > baseline
                        && snapshot.tick_lateness_micros.len() >= 80
                }) => snapshot,
            };
            assert!(snapshot.error.is_none());
            assert_eq!(snapshot.failed_probes, 0);
            assert!(
                snapshot
                    .records
                    .iter()
                    .all(|record| record.state == MembershipState::Alive)
            );
            let mut tick_lateness = snapshot.tick_lateness_micros.clone();
            tick_lateness.sort_unstable();
            let p50 = tick_lateness[(tick_lateness.len() * 50 - 1) / 100];
            let p95 = tick_lateness[(tick_lateness.len() * 95 - 1) / 100];
            eprintln!(
                "cpu-bound-control {}",
                serde_json::json!({"elapsed_ms": started.elapsed().as_millis(),
                "successful_probes": snapshot.successful_probes, "failed_probes": snapshot.failed_probes,
                "tick_samples": tick_lateness.len(), "tick_lateness_p50_us": p50,
                "tick_lateness_p95_us": p95, "tick_lateness_max_us": tick_lateness.last(),
                "health": snapshot.health})
            );
        }
        guest_runtime.shutdown().await.unwrap();
        alice.stop().await;
        bob.stop().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
    async fn production_adapter_shutdown_is_bounded_while_storage_is_paused() {
        let format = nx_net::SerializationFormat::Bincode;
        let mut alice = Running::new("alice", format).await;
        let mut bob = Running::new("bob", format).await;
        alice.node.connect_to_peer(&bob.endpoint).await.unwrap();
        alice.wait(|snapshot| snapshot.records.len() == 2).await;
        let mut owner = bob.owner("bob").await;
        owner.state = MembershipState::Left;
        let (reached_tx, reached_rx) = std::sync::mpsc::channel();
        let (resume_tx, resume_rx) = std::sync::mpsc::channel();
        alice.store.pause_next_flush(reached_tx, resume_rx);
        bob.node
            .control_request(
                &alice.endpoint,
                ControlRequest {
                    request_id: 9300,
                    owner,
                    updates: vec![],
                    kind: ControlKind::Exchange { after: None },
                },
                Duration::from_secs(2),
            )
            .await
            .unwrap();
        tokio::task::spawn_blocking(move || reached_rx.recv_timeout(Duration::from_secs(5)))
            .await
            .unwrap()
            .unwrap();
        let handle = alice.handle.take().unwrap();
        let snapshot = handle.snapshot.clone();
        let result = tokio::time::timeout(
            Duration::from_secs(2),
            handle.shutdown_with_budget(Duration::from_millis(50)),
        )
        .await
        .unwrap();
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("shutdown budget exhausted")
        );
        resume_tx.send(()).unwrap();
        let store = Arc::clone(&alice.store);
        tokio::task::spawn_blocking(move || store.flush())
            .await
            .unwrap()
            .unwrap();
        assert!(
            !snapshot
                .borrow()
                .records
                .iter()
                .any(|record| record.member == "alice" && record.state == MembershipState::Left)
        );
        alice.node.shutdown().await;
        bob.stop().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
    async fn production_adapter_detects_crash_and_helper_sends_nack() {
        let format = nx_net::SerializationFormat::Bincode;
        let mut alice = Running::new("alice", format).await;
        let mut bob = Running::new("bob", format).await;
        let mut carol = Running::new("carol", format).await;
        alice.node.connect_to_peer(&bob.endpoint).await.unwrap();
        carol.node.connect_to_peer(&bob.endpoint).await.unwrap();
        for runtime in [&mut alice, &mut bob, &mut carol] {
            runtime
                .wait(|snapshot| snapshot.records.len() == 3 && snapshot.successful_probes > 0)
                .await;
        }
        let owner = alice.owner("alice").await;
        let target = carol.owner("carol").await;
        drop(carol.handle.take());
        carol.node.shutdown().await;
        let reply = alice
            .node
            .control_request(
                &bob.endpoint,
                ControlRequest {
                    request_id: 9100,
                    owner,
                    updates: vec![],
                    kind: ControlKind::Indirect {
                        target: target.member,
                        version: target.version,
                    },
                },
                Duration::from_secs(2),
            )
            .await
            .unwrap()
            .1;
        assert!(!reply.ack);
        assert!(reply.nack);
        let snapshot = alice
            .wait(|snapshot| {
                snapshot
                    .records
                    .iter()
                    .any(|record| record.member == "carol" && record.state == MembershipState::Dead)
            })
            .await;
        assert!(snapshot.error.is_none());
        assert!(snapshot.failed_probes > 0);
        assert!(
            alice
                .store
                .get(b"__nx/membership/floor/carol")
                .unwrap()
                .is_some()
        );
        alice.stop().await;
        bob.stop().await;
        carol.stop().await;
    }
}
