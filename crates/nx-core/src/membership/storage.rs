use std::fmt;

use nx_store::{Store, StoreWriteLease};

use super::Error;
use super::schema::{self, FLOOR_PREFIX, Floor, HEADER, LOCAL_KEY, PREFIX, SCHEMA_KEY};
use super::state::{Action, Membership, Status};

const RECORD_BYTES: usize = 2048;
const PAGE_RECORDS: u32 = 64;

#[derive(Debug)]
pub(super) enum StorageError {
    Store(nx_store::StoreError),
    Core(Error),
}

impl fmt::Display for StorageError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Store(error) => write!(formatter, "membership storage: {error}"),
            Self::Core(error) => write!(formatter, "membership metadata: {error:?}"),
        }
    }
}

impl std::error::Error for StorageError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Store(error) => Some(error),
            Self::Core(_) => None,
        }
    }
}

impl From<nx_store::StoreError> for StorageError {
    fn from(error: nx_store::StoreError) -> Self {
        Self::Store(error)
    }
}

impl From<Error> for StorageError {
    fn from(error: Error) -> Self {
        Self::Core(error)
    }
}

pub(super) struct MembershipStore<'store> {
    store: &'store Store,
}

impl<'store> MembershipStore<'store> {
    pub(super) fn new(store: &'store Store) -> Self {
        Self { store }
    }

    pub(super) fn start(&self, membership: &mut Membership) -> Result<Vec<Action>, StorageError> {
        let result = self.start_inner(membership);
        if result.is_err() {
            membership.fence();
        }
        result
    }

    fn start_inner(&self, membership: &mut Membership) -> Result<Vec<Action>, StorageError> {
        let lease = self.store.acquire_write_lease()?;
        let (previous_epoch, floors) =
            Self::load(&lease, membership.local_id(), membership.capacity())?;
        membership.restore(&floors)?;
        let Action::PersistEpoch { token, epoch } = membership.start(previous_epoch)? else {
            return Err(Error::NotReady.into());
        };
        let local = schema::encode_local(membership.local_id(), epoch)?;
        let result = lease
            .apply_batch(&[(SCHEMA_KEY, HEADER), (LOCAL_KEY, &local)], &[])
            .and_then(|()| lease.flush());
        if let Err(error) = result {
            let _ = membership.confirm_start(token, false);
            return Err(error.into());
        }
        Ok(membership.confirm_start(token, true)?)
    }

    fn load(
        lease: &StoreWriteLease<'_>,
        local_id: &str,
        capacity: usize,
    ) -> Result<(u64, Vec<Floor>), StorageError> {
        let mut header = None;
        let mut local = None;
        let mut floors = Vec::new();
        let mut cursor = None;
        let mut count = 0;
        loop {
            let page = lease.store().scan_prefix_page_after_bounded(
                PREFIX,
                cursor.as_deref(),
                PAGE_RECORDS,
                PAGE_RECORDS as usize * RECORD_BYTES,
            )?;
            if page.is_empty() {
                break;
            }
            for (key, value) in page {
                count += 1;
                if count > capacity + 3 {
                    return Err(Error::Capacity.into());
                }
                if key.len() + value.len() > RECORD_BYTES {
                    return Err(Error::InvalidMetadata.into());
                }
                if key == SCHEMA_KEY {
                    if value != HEADER {
                        return Err(Error::InvalidMetadata.into());
                    }
                    header = Some(());
                } else if key == LOCAL_KEY {
                    let (owner, epoch) = schema::decode_local(&value)?;
                    if owner != local_id {
                        return Err(Error::Unauthorized.into());
                    }
                    local = Some(epoch);
                } else if let Some(member) = key.strip_prefix(FLOOR_PREFIX) {
                    let member = std::str::from_utf8(member).map_err(|_| Error::InvalidMetadata)?;
                    floors.push(Floor::decode(member, &value)?);
                } else {
                    return Err(Error::InvalidMetadata.into());
                }
                cursor = Some(key);
            }
        }
        if count == 0 {
            return Ok((0, floors));
        }
        if header.is_none() {
            return Err(Error::InvalidMetadata.into());
        }
        let epoch = local.ok_or(Error::InvalidMetadata)?;
        for floor in &floors {
            if floor.member == local_id
                && (floor.status != Status::Left || floor.version.epoch > epoch)
            {
                return Err(Error::InvalidMetadata.into());
            }
        }
        floors.retain(|floor| floor.member != local_id);
        if floors.len() > capacity {
            return Err(Error::Capacity.into());
        }
        Ok((epoch, floors))
    }

    #[cfg(test)]
    pub(super) fn persist(
        &self,
        membership: &mut Membership,
        action: &Action,
    ) -> Result<Vec<Action>, StorageError> {
        if !membership.pending_floor(action) {
            return Ok(Vec::new());
        }
        let Action::PersistFloor { token, update } = action else {
            return Err(Error::NotReady.into());
        };
        let result = self.persist_update(
            membership.local_id(),
            membership.local_epoch().ok_or(Error::NotReady)?,
            update,
        );
        if let Err(error) = result {
            let _ = membership.confirm_floor(*token, false);
            membership.fence();
            return Err(error);
        }
        Ok(membership.confirm_floor(*token, true)?)
    }

    pub(super) fn persist_update(
        &self,
        local_id: &str,
        local_epoch: u64,
        update: &super::state::Update,
    ) -> Result<(), StorageError> {
        let lease = self.store.acquire_write_lease()?;
        if lease.store().get(SCHEMA_KEY)?.as_deref() != Some(HEADER) {
            return Err(Error::InvalidMetadata.into());
        }
        let local = lease
            .store()
            .get(LOCAL_KEY)?
            .ok_or(Error::InvalidMetadata)?;
        let (owner, epoch) = schema::decode_local(&local)?;
        if owner != local_id || epoch != local_epoch {
            return Err(Error::Unauthorized.into());
        }
        let incoming = Floor::from_update(update)?;
        let key = incoming.key();
        let floor = match lease.store().get(&key)? {
            Some(bytes) => Floor::decode(&incoming.member, &bytes)?.merge(&incoming)?,
            None => incoming,
        };
        let value = floor.encode()?;
        lease.apply_batch(&[(&key, &value)], &[])?;
        lease.flush()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::membership::Version;
    use crate::membership::state::Update;
    use crate::membership::suspicion::tests::timing;

    fn machine() -> Membership {
        Membership::new("alice".into(), 3, timing(), 2, 1).unwrap()
    }

    fn version(epoch: u64, incarnation: u64) -> Version {
        Version { epoch, incarnation }
    }

    fn owner_left(machine: &mut Membership) -> Action {
        machine
            .admit("bob", version(7, 2), 11, Duration::ZERO)
            .unwrap();
        machine
            .owner_leave("bob", 11, version(7, 2), Duration::ZERO)
            .unwrap()
            .remove(0)
    }

    #[test]
    fn restart_advances_epoch_and_restores_left_without_sessions() {
        let directory = tempfile::tempdir().unwrap();
        {
            let store = Store::open(directory.path()).unwrap();
            store.set(b"application", b"untouched").unwrap();
            let adapter = MembershipStore::new(&store);
            let mut machine = machine();
            assert!(matches!(
                adapter.start(&mut machine).unwrap().as_slice(),
                [Action::Publish(Update {
                    version: Version { epoch: 1, .. },
                    ..
                })]
            ));
            let left = owner_left(&mut machine);
            assert!(matches!(
                adapter.persist(&mut machine, &left).unwrap().as_slice(),
                [Action::Publish(Update {
                    status: Status::Left,
                    ..
                })]
            ));
        }
        let store = Store::open(directory.path()).unwrap();
        let adapter = MembershipStore::new(&store);
        let mut machine = machine();
        adapter.start(&mut machine).unwrap();
        assert_eq!(machine.local_epoch(), Some(2));
        assert!(
            machine
                .admit("bob", version(7, 99), 12, Duration::ZERO)
                .unwrap()
                .is_empty()
        );
        assert!(
            machine
                .owner_alive("bob", 11, version(8, 0), Duration::ZERO)
                .is_err()
        );
        assert!(
            !machine
                .admit("bob", version(8, 0), 12, Duration::ZERO)
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            store.get(b"application").unwrap(),
            Some(b"untouched".to_vec())
        );
    }

    #[test]
    fn superseded_action_is_not_written_and_completed_action_is_idempotent() {
        let directory = tempfile::tempdir().unwrap();
        let store = Store::open(directory.path()).unwrap();
        let adapter = MembershipStore::new(&store);
        let mut machine = machine();
        adapter.start(&mut machine).unwrap();
        let left = owner_left(&mut machine);
        machine
            .admit("bob", version(8, 0), 12, Duration::ZERO)
            .unwrap();
        assert!(adapter.persist(&mut machine, &left).unwrap().is_empty());
        assert!(!store.prefix_exists(FLOOR_PREFIX).unwrap());
        let left = machine
            .owner_leave("bob", 12, version(8, 0), Duration::ZERO)
            .unwrap()
            .remove(0);
        adapter.persist(&mut machine, &left).unwrap();
        let flushes = store.completed_flushes();
        assert!(adapter.persist(&mut machine, &left).unwrap().is_empty());
        assert_eq!(store.completed_flushes(), flushes);
    }

    #[test]
    fn write_and_flush_failures_keep_original_cause_and_fence_publication() {
        for fail_flush in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let store = Store::open(directory.path()).unwrap();
            let adapter = MembershipStore::new(&store);
            let mut machine = machine();
            adapter.start(&mut machine).unwrap();
            let left = owner_left(&mut machine);
            if fail_flush {
                store.inject_disk_full_on_flush(true);
            } else {
                store.inject_disk_full_on_writes(true);
            }
            let error = adapter.persist(&mut machine, &left).unwrap_err();
            assert!(matches!(
                (fail_flush, error),
                (
                    true,
                    StorageError::Store(nx_store::StoreError::InjectedFlushFailure)
                ) | (
                    false,
                    StorageError::Store(nx_store::StoreError::InjectedDiskFull)
                )
            ));
            assert_eq!(
                machine.probe_next(Duration::ZERO, |_| 0),
                Err(Error::NotReady)
            );
            assert_eq!(store.prefix_exists(FLOOR_PREFIX).unwrap(), fail_flush);
        }
    }

    #[test]
    fn wrong_identity_unknown_schema_and_orphaned_records_fail_without_writes() {
        for record in [
            (SCHEMA_KEY, b"NXMB\0\x02\0\0".as_slice()),
            (LOCAL_KEY, br#"{"member":"someone-else","epoch":4}"#),
            (b"__nx/membership/unknown", b"bad"),
        ] {
            let directory = tempfile::tempdir().unwrap();
            let store = Store::open(directory.path()).unwrap();
            store.set(record.0, record.1).unwrap();
            store.flush().unwrap();
            let before = store.scan_prefix(PREFIX).unwrap();
            assert!(MembershipStore::new(&store).start(&mut machine()).is_err());
            assert_eq!(store.scan_prefix(PREFIX).unwrap(), before);
        }
    }

    #[test]
    fn startup_failures_do_not_announce_and_reopen_covers_surviving_batch() {
        for fail_flush in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            {
                let store = Store::open(directory.path()).unwrap();
                if fail_flush {
                    store.inject_disk_full_on_flush(true);
                } else {
                    store.inject_disk_full_on_writes(true);
                }
                let mut machine = machine();
                assert!(matches!(
                    MembershipStore::new(&store).start(&mut machine),
                    Err(StorageError::Store(_))
                ));
                assert_eq!(machine.local_epoch(), None);
                assert_eq!(machine.start(0), Err(Error::NotReady));
                assert_eq!(store.exists(SCHEMA_KEY).unwrap(), fail_flush);
                assert_eq!(store.exists(LOCAL_KEY).unwrap(), fail_flush);
            }
            let store = Store::open(directory.path()).unwrap();
            let previous = store
                .get(LOCAL_KEY)
                .unwrap()
                .map(|bytes| schema::decode_local(&bytes).unwrap().1)
                .unwrap_or(0);
            let mut machine = machine();
            MembershipStore::new(&store).start(&mut machine).unwrap();
            assert_eq!(machine.local_epoch(), Some(previous + 1));
        }
    }

    #[test]
    fn covering_flush_blocks_both_initial_and_terminal_publication() {
        use std::sync::mpsc;

        for terminal in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let store = Store::open(directory.path()).unwrap();
            let mut machine = machine();
            let action = if terminal {
                MembershipStore::new(&store).start(&mut machine).unwrap();
                Some(owner_left(&mut machine))
            } else {
                None
            };
            let (reached_tx, reached_rx) = mpsc::channel();
            let (resume_tx, resume_rx) = mpsc::channel();
            let (published_tx, published_rx) = mpsc::channel();
            store.pause_next_flush(reached_tx, resume_rx);
            std::thread::scope(|scope| {
                scope.spawn(|| {
                    let adapter = MembershipStore::new(&store);
                    let actions = match &action {
                        Some(action) => adapter.persist(&mut machine, action),
                        None => adapter.start(&mut machine),
                    }
                    .unwrap();
                    published_tx.send(actions).unwrap();
                });
                let reached = reached_rx.recv_timeout(Duration::from_secs(10));
                let before_flush = published_rx.try_recv();
                let written = if terminal {
                    store.prefix_exists(FLOOR_PREFIX)
                } else {
                    store.exists(LOCAL_KEY)
                };
                let resumed = resume_tx.send(());
                reached.unwrap();
                resumed.unwrap();
                assert!(matches!(before_flush, Err(mpsc::TryRecvError::Empty)));
                assert!(written.unwrap());
                assert!(
                    published_rx
                        .recv_timeout(Duration::from_secs(10))
                        .unwrap()
                        .iter()
                        .any(|action| matches!(action, Action::Publish(_)))
                );
            });
        }
    }

    #[test]
    fn reopening_after_uncertain_floor_flush_preserves_surviving_rejection() {
        let directory = tempfile::tempdir().unwrap();
        {
            let store = Store::open(directory.path()).unwrap();
            let adapter = MembershipStore::new(&store);
            let mut machine = machine();
            adapter.start(&mut machine).unwrap();
            let left = owner_left(&mut machine);
            store.inject_disk_full_on_flush(true);
            assert!(matches!(
                adapter.persist(&mut machine, &left),
                Err(StorageError::Store(
                    nx_store::StoreError::InjectedFlushFailure
                ))
            ));
            store.inject_disk_full_on_flush(false);
            assert!(
                MembershipStore::new(&store)
                    .start(&mut self::machine())
                    .is_err()
            );
            assert_eq!(machine.local_epoch(), Some(1));
        }
        let store = Store::open(directory.path()).unwrap();
        let survived = store.prefix_exists(FLOOR_PREFIX).unwrap();
        let mut machine = machine();
        MembershipStore::new(&store).start(&mut machine).unwrap();
        assert_eq!(machine.local_epoch(), Some(2));
        assert_eq!(
            machine
                .admit("bob", version(7, 99), 12, Duration::ZERO)
                .unwrap()
                .is_empty(),
            survived
        );
    }

    #[test]
    fn newer_terminal_epoch_retains_previous_leave_and_local_leave_survives_restart() {
        let directory = tempfile::tempdir().unwrap();
        {
            let store = Store::open(directory.path()).unwrap();
            let adapter = MembershipStore::new(&store);
            let mut machine = machine();
            adapter.start(&mut machine).unwrap();
            let left = owner_left(&mut machine);
            adapter.persist(&mut machine, &left).unwrap();
            machine
                .admit("bob", version(8, 0), 12, Duration::ZERO)
                .unwrap();
            machine
                .admit("carol", version(1, 0), 13, Duration::ZERO)
                .unwrap();
            let dead = machine
                .report_dead("carol", 13, "bob", version(8, 0), Duration::ZERO)
                .unwrap()
                .remove(0);
            adapter.persist(&mut machine, &dead).unwrap();
            let floor = Floor::decode(
                "bob",
                &store.get(b"__nx/membership/floor/bob").unwrap().unwrap(),
            )
            .unwrap();
            assert_eq!(floor.closed_epoch, Some(7));
            assert_eq!(floor.status, Status::Dead);
            let local_left = machine
                .leave()
                .unwrap()
                .into_iter()
                .find(|action| matches!(action, Action::PersistFloor { .. }))
                .unwrap();
            adapter.persist(&mut machine, &local_left).unwrap();
        }
        let store = Store::open(directory.path()).unwrap();
        let mut machine = machine();
        MembershipStore::new(&store).start(&mut machine).unwrap();
        assert_eq!(machine.local_epoch(), Some(2));
        assert!(
            machine
                .admit("bob", version(8, 0), 14, Duration::ZERO)
                .unwrap()
                .is_empty()
        );
        assert!(
            !machine
                .admit("bob", version(8, 1), 14, Duration::ZERO)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn an_older_instance_cannot_write_after_another_start_advanced_epoch() {
        let directory = tempfile::tempdir().unwrap();
        let store = Store::open(directory.path()).unwrap();
        let adapter = MembershipStore::new(&store);
        let mut first = machine();
        adapter.start(&mut first).unwrap();
        let left = owner_left(&mut first);
        adapter.start(&mut machine()).unwrap();
        assert!(matches!(
            adapter.persist(&mut first, &left),
            Err(StorageError::Core(Error::Unauthorized))
        ));
        assert!(!store.prefix_exists(FLOOR_PREFIX).unwrap());
        assert_eq!(
            first.probe_next(Duration::ZERO, |_| 0),
            Err(Error::NotReady)
        );
    }

    #[test]
    fn paginated_hydration_respects_capacity_without_evicting_floors() {
        let directory = tempfile::tempdir().unwrap();
        let store = Store::open(directory.path()).unwrap();
        MembershipStore::new(&store).start(&mut machine()).unwrap();
        for index in 0..70 {
            let floor = Floor::from_update(&Update {
                member: format!("node-{index}"),
                version: version(1, 0),
                status: Status::Dead,
            })
            .unwrap();
            store.set(&floor.key(), &floor.encode().unwrap()).unwrap();
        }
        store.flush().unwrap();
        let before = store.scan_prefix(PREFIX).unwrap();
        let mut too_small = Membership::new("alice".into(), 69, timing(), 2, 1).unwrap();
        assert!(matches!(
            MembershipStore::new(&store).start(&mut too_small),
            Err(StorageError::Core(Error::Capacity))
        ));
        assert_eq!(store.scan_prefix(PREFIX).unwrap(), before);
        let mut enough = Membership::new("alice".into(), 70, timing(), 2, 1).unwrap();
        MembershipStore::new(&store).start(&mut enough).unwrap();
        assert!(enough.probe_next(Duration::ZERO, |_| 0).unwrap().is_empty());
        assert_eq!(
            enough.admit("extra", version(1, 0), 1, Duration::ZERO),
            Err(Error::Capacity)
        );
    }

    #[test]
    fn corrupt_metadata_exhausted_epoch_and_identity_mismatch_never_mutate_store() {
        for (key, value) in [
            (SCHEMA_KEY.to_vec(), b"NXMB\0\x02\0\0".to_vec()),
            (
                LOCAL_KEY.to_vec(),
                schema::encode_local("other", 1).unwrap(),
            ),
            (
                LOCAL_KEY.to_vec(),
                schema::encode_local("alice", u64::MAX).unwrap(),
            ),
            (b"__nx/membership/floor/bob".to_vec(), b"corrupt".to_vec()),
            (
                b"__nx/membership/floor/bob".to_vec(),
                vec![b' '; RECORD_BYTES + 1],
            ),
        ] {
            let directory = tempfile::tempdir().unwrap();
            let store = Store::open(directory.path()).unwrap();
            MembershipStore::new(&store).start(&mut machine()).unwrap();
            store.set(&key, &value).unwrap();
            store.flush().unwrap();
            let before = store.scan_prefix(PREFIX).unwrap();
            assert!(MembershipStore::new(&store).start(&mut machine()).is_err());
            assert_eq!(store.scan_prefix(PREFIX).unwrap(), before);
        }
    }

    #[test]
    fn membership_crash_child() {
        let Some(path) = std::env::var_os("NUMAX_MEMBERSHIP_CRASH_STORE") else {
            return;
        };
        let store = Store::open(path).unwrap();
        let adapter = MembershipStore::new(&store);
        let mut machine = machine();
        let terminal = std::env::var("NUMAX_MEMBERSHIP_CRASH_PHASE").unwrap() == "left";
        if terminal {
            adapter.start(&mut machine).unwrap();
        }
        let checkpoint = match std::env::var("NUMAX_MEMBERSHIP_CRASH_POINT")
            .unwrap()
            .as_str()
        {
            "before_batch" => nx_store::CrashCheckpoint::BeforeBatch,
            "after_batch" => nx_store::CrashCheckpoint::AfterBatch,
            "before_flush" => nx_store::CrashCheckpoint::BeforeFlush,
            "after_flush" => nx_store::CrashCheckpoint::AfterFlush,
            _ => panic!("unknown checkpoint"),
        };
        store.inject_crash_checkpoint(
            checkpoint,
            std::env::var_os("NUMAX_MEMBERSHIP_CRASH_MARKER")
                .unwrap()
                .into(),
        );
        if terminal {
            let left = owner_left(&mut machine);
            adapter.persist(&mut machine, &left).unwrap();
        } else {
            adapter.start(&mut machine).unwrap();
        }
        panic!("membership passed armed checkpoint");
    }

    #[test]
    fn process_crash_preserves_atomic_metadata_and_flush_confirmed_floors() {
        struct Child(std::process::Child);
        impl Drop for Child {
            fn drop(&mut self) {
                let _ = self.0.kill();
                let _ = self.0.wait();
            }
        }
        for phase in ["initial", "restart", "left"] {
            for checkpoint in ["before_batch", "after_batch", "before_flush", "after_flush"] {
                let directory = tempfile::tempdir().unwrap();
                let path = directory.path().join("store");
                let marker = directory.path().join("checkpoint");
                {
                    let store = Store::open(&path).unwrap();
                    store.set(b"application", b"untouched").unwrap();
                    store.flush().unwrap();
                    if phase != "initial" {
                        let adapter = MembershipStore::new(&store);
                        let mut machine = machine();
                        adapter.start(&mut machine).unwrap();
                        if phase == "restart" {
                            let left = owner_left(&mut machine);
                            adapter.persist(&mut machine, &left).unwrap();
                        }
                    }
                }
                let mut child = Child(
                    std::process::Command::new(std::env::current_exe().unwrap())
                        .args([
                            "--exact",
                            "membership::storage::tests::membership_crash_child",
                        ])
                        .env("NUMAX_MEMBERSHIP_CRASH_STORE", &path)
                        .env("NUMAX_MEMBERSHIP_CRASH_PHASE", phase)
                        .env("NUMAX_MEMBERSHIP_CRASH_POINT", checkpoint)
                        .env("NUMAX_MEMBERSHIP_CRASH_MARKER", &marker)
                        .stdout(std::process::Stdio::null())
                        .spawn()
                        .unwrap(),
                );
                let deadline = std::time::Instant::now() + Duration::from_secs(10);
                while !marker.exists() {
                    assert!(
                        child.0.try_wait().unwrap().is_none(),
                        "child exited before {phase}/{checkpoint}"
                    );
                    assert!(
                        std::time::Instant::now() < deadline,
                        "checkpoint not reached: {phase}/{checkpoint}"
                    );
                    std::thread::yield_now();
                }
                child.0.kill().unwrap();
                child.0.wait().unwrap();
                let store = Store::open(&path).unwrap();
                let local = store.get(LOCAL_KEY).unwrap();
                assert_eq!(store.exists(SCHEMA_KEY).unwrap(), local.is_some());
                if phase == "initial" && checkpoint == "before_batch" {
                    assert!(local.is_none());
                }
                if checkpoint == "after_flush" {
                    assert!(local.is_some());
                }
                let previous = local
                    .map(|bytes| schema::decode_local(&bytes).unwrap().1)
                    .unwrap_or(0);
                match phase {
                    "initial" => assert!(previous <= 1),
                    "restart" => assert!([1, 2].contains(&previous)),
                    "left" => assert_eq!(previous, 2),
                    _ => unreachable!(),
                }
                let survived_floor = store.prefix_exists(FLOOR_PREFIX).unwrap();
                if phase == "restart" || phase == "left" && checkpoint == "after_flush" {
                    assert!(survived_floor);
                }
                if phase == "left" && checkpoint == "before_batch" {
                    assert!(!survived_floor);
                }
                let mut machine = machine();
                MembershipStore::new(&store).start(&mut machine).unwrap();
                assert_eq!(machine.local_epoch(), Some(previous + 1));
                if survived_floor {
                    assert!(
                        machine
                            .admit("bob", version(7, 99), 12, Duration::ZERO)
                            .unwrap()
                            .is_empty()
                    );
                }
                assert_eq!(
                    store.get(b"application").unwrap(),
                    Some(b"untouched".to_vec())
                );
            }
        }
    }
}
