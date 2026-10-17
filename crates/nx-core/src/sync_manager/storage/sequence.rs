use std::collections::BTreeMap;

use nx_store::{Store as NxStore, StoreWriteLease};
use nx_sync::{HistorySummary, NodeId, Op, OpPosition, SequencedOp};

use super::super::sequence::*;

pub(in crate::sync_manager) const SEQUENCE_PREFIX: &str = "__nx/crdt/sequence/";
const LOCAL: &[u8] = b"__nx/crdt/sequence/local";
const USAGE: &[u8] = b"__nx/crdt/sequence/usage";
const ORIGINS: &[u8] = b"__nx/crdt/sequence/origin/";
const POSITIONS: &[u8] = b"__nx/crdt/sequence/replay/";

pub(in crate::sync_manager) type SequenceSets = Vec<(Vec<u8>, Vec<u8>)>;
type LocalAllocator = (String, u64, u64);
type Usage = (usize, usize, usize); // origins, serialized tracker bytes, sparse entries

fn origin_key(origin: &str) -> anyhow::Result<Vec<u8>> {
    anyhow::ensure!(
        !origin.is_empty() && origin.len() <= 256,
        "invalid origin identity length"
    );
    Ok([ORIGINS, origin.as_bytes()].concat())
}

pub(in crate::sync_manager) fn position_key(id: &str) -> Vec<u8> {
    [POSITIONS, id.as_bytes()].concat()
}

fn usage(store: &NxStore) -> anyhow::Result<Usage> {
    match store.get(USAGE)? {
        Some(bytes) => Ok(serde_json::from_slice(&bytes)?),
        None => {
            anyhow::ensure!(
                !store.prefix_exists(ORIGINS)?,
                "missing tracking usage metadata"
            );
            Ok((0, 0, 0))
        }
    }
}

fn local(store: &NxStore) -> anyhow::Result<Option<LocalAllocator>> {
    store
        .get(LOCAL)?
        .map(|bytes| serde_json::from_slice(&bytes).map_err(Into::into))
        .transpose()
}

fn tracker(store: &NxStore, origin: &str) -> anyhow::Result<ReceiptTracker> {
    match store.get(&origin_key(origin)?)? {
        Some(bytes) => ReceiptTracker::decode(&bytes),
        None => Ok(ReceiptTracker::default()),
    }
}

fn tracker_sets(
    store: &NxStore,
    updates: BTreeMap<String, ReceiptTracker>,
) -> anyhow::Result<SequenceSets> {
    let (mut origins, mut bytes, mut sparse) = usage(store)?;
    let mut sets = Vec::new();
    let mut additions = (0usize, 0usize);
    for (origin, progress) in updates {
        let key = origin_key(&origin)?;
        if let Some(old) = store.get(&key)? {
            bytes = bytes
                .checked_sub(key.len() + old.len())
                .ok_or_else(|| anyhow::anyhow!("corrupt tracking byte usage"))?;
            sparse = sparse
                .checked_sub(ReceiptTracker::decode(&old)?.sparse.len())
                .ok_or_else(|| anyhow::anyhow!("corrupt tracking sparse usage"))?;
        } else {
            origins = origins
                .checked_add(1)
                .ok_or_else(|| anyhow::anyhow!("origin count overflow"))?;
        }
        let value = progress.encode()?;
        additions.0 = additions
            .0
            .checked_add(key.len() + value.len())
            .ok_or_else(|| anyhow::anyhow!("tracking bytes overflow"))?;
        additions.1 = additions
            .1
            .checked_add(progress.sparse.len())
            .ok_or_else(|| anyhow::anyhow!("sparse count overflow"))?;
        sets.push((key, value));
    }
    bytes = bytes
        .checked_add(additions.0)
        .ok_or_else(|| anyhow::anyhow!("tracking bytes overflow"))?;
    sparse = sparse
        .checked_add(additions.1)
        .ok_or_else(|| anyhow::anyhow!("sparse count overflow"))?;
    if origins > MAX_ORIGINS || bytes > MAX_TRACKING_BYTES || sparse > MAX_TOTAL_SPARSE {
        return Err(TrackingLimit::GlobalBudget.into());
    }
    sets.push((
        USAGE.to_vec(),
        serde_json::to_vec(&(origins, bytes, sparse))?,
    ));
    Ok(sets)
}

pub(in crate::sync_manager) fn initialize_sequence_origin(
    store: &NxStore,
    origin: &NodeId,
) -> anyhow::Result<u64> {
    origin_key(origin.as_str())?;
    let lease = store.acquire_write_lease()?;
    // Verify compacted frontiers and accounting before admitting any new writes.
    let mut cursor = None;
    let mut actual = (0usize, 0usize, 0usize);
    loop {
        let page = store.scan_prefix_page_after_bounded(
            ORIGINS,
            cursor.as_deref(),
            SUMMARY_PAGE as u32,
            MAX_TRACKING_BYTES,
        )?;
        if page.is_empty() {
            break;
        }
        for (key, bytes) in &page {
            let origin = std::str::from_utf8(&key[ORIGINS.len()..])?;
            origin_key(origin)?;
            let progress = ReceiptTracker::decode(bytes)?;
            actual.0 += 1;
            actual.1 += key.len() + bytes.len();
            actual.2 += progress.sparse.len();
            anyhow::ensure!(
                actual.0 <= MAX_ORIGINS
                    && actual.1 <= MAX_TRACKING_BYTES
                    && actual.2 <= MAX_TOTAL_SPARSE,
                "stored history exceeds tracking budget"
            );
        }
        cursor = page.last().map(|(key, _)| key.clone());
    }
    anyhow::ensure!(
        actual == usage(store)?,
        "history tracking accounting mismatch"
    );
    let (_, previous_generation, sequence) = match local(store)? {
        Some((id, generation, seq)) => {
            anyhow::ensure!(
                id == origin.as_str() && generation > 0,
                "sequence store belongs to a different origin or has invalid generation"
            );
            let progress = tracker(store, &id)?;
            anyhow::ensure!(
                progress.contiguous == seq && progress.sparse.is_empty(),
                "local sequence/frontier mismatch"
            );
            (id, generation, seq)
        }
        None => {
            anyhow::ensure!(
                !store.prefix_exists(&origin_key(origin.as_str())?)?,
                "missing local sequence allocator"
            );
            (origin.as_str().to_owned(), 0, 0)
        }
    };
    let generation = previous_generation
        .checked_add(1)
        .ok_or_else(|| anyhow::anyhow!("origin generation exhausted"))?;
    let value = serde_json::to_vec(&(origin.as_str(), generation, sequence))?;
    let mut sets = Vec::new();
    if previous_generation == 0 {
        let mut has_legacy = false;
        for table in super::super::schema::StoreTable::ALL {
            if table != super::super::schema::StoreTable::Sequences
                && store.prefix_exists(table.data_prefix().as_bytes())?
            {
                has_legacy = true;
                break;
            }
        }
        if has_legacy {
            let progress = ReceiptTracker {
                baseline_required: true,
                ..ReceiptTracker::default()
            };
            sets = tracker_sets(
                store,
                BTreeMap::from([(origin.as_str().to_owned(), progress)]),
            )?;
        }
    }
    sets.push((LOCAL.to_vec(), value));
    let refs = sets
        .iter()
        .map(|(k, v)| (k.as_slice(), v.as_slice()))
        .collect::<Vec<_>>();
    lease.apply_batch(&refs, &[])?;
    lease.flush()?;
    Ok(generation)
}

pub(in crate::sync_manager) fn local_sequence_sets(
    lease: &StoreWriteLease<'_>,
    op: &Op,
    expected_generation: u64,
) -> anyhow::Result<SequenceSets> {
    let store = lease.store();
    let (origin, generation, previous) =
        local(store)?.ok_or_else(|| anyhow::anyhow!("uninitialized sequence allocator"))?;
    anyhow::ensure!(
        origin == op.origin.as_str() && generation == expected_generation,
        "stale local sequence generation"
    );
    let sequence = previous
        .checked_add(1)
        .ok_or_else(|| anyhow::anyhow!("origin sequence exhausted"))?;
    let position = OpPosition {
        generation,
        sequence,
    };
    anyhow::ensure!(
        serde_json::to_vec(&SequencedOp {
            op: op.clone(),
            position
        })?
        .len()
            + 1024
            <= REPLAY_BYTES,
        "operation exceeds replay byte budget"
    );
    let mut progress = tracker(store, &origin)?;
    anyhow::ensure!(
        progress.contiguous == previous,
        "local sequence/frontier mismatch"
    );
    anyhow::ensure!(
        progress.observe(position, op.id.as_str())?,
        "local operation already accounted for"
    );
    let mut sets = tracker_sets(store, BTreeMap::from([(origin.clone(), progress)]))?;
    sets.push((
        LOCAL.to_vec(),
        serde_json::to_vec(&(&origin, generation, sequence))?,
    ));
    sets.push((
        position_key(op.id.as_str()),
        serde_json::to_vec(&(origin, generation, sequence))?,
    ));
    Ok(sets)
}

pub(in crate::sync_manager) fn sequenced_op(
    store: &NxStore,
    op: &Op,
) -> anyhow::Result<Option<SequencedOp>> {
    let Some(bytes) = store.get(&position_key(op.id.as_str()))? else {
        return Ok(None);
    };
    let (origin, generation, sequence): LocalAllocator = serde_json::from_slice(&bytes)?;
    anyhow::ensure!(
        origin == op.origin.as_str() && generation > 0 && sequence > 0,
        "invalid replay position"
    );
    Ok(Some(SequencedOp {
        op: op.clone(),
        position: OpPosition {
            generation,
            sequence,
        },
    }))
}

pub(in crate::sync_manager) fn remote_sequence_sets(
    lease: &StoreWriteLease<'_>,
    operations: &[SequencedOp],
) -> anyhow::Result<(Vec<SequencedOp>, SequenceSets)> {
    anyhow::ensure!(
        operations.len() <= 1024,
        "sequenced batch exceeds operation budget"
    );
    let store = lease.store();
    let allocator = local(store)?;
    let mut updates = BTreeMap::new();
    let mut identities = BTreeMap::new();
    let mut positions = BTreeMap::new();
    // Validate the entire envelope before a recoverable resource rejection can be returned.
    for entry in operations {
        let origin = entry.op.origin.as_str();
        origin_key(origin)?;
        let progress = tracker(store, origin)?;
        progress.validate_identity(entry.position, entry.op.id.as_str())?;
        anyhow::ensure!(
            serde_json::to_vec(entry)?.len() + 1024 <= REPLAY_BYTES,
            "operation exceeds replay byte budget"
        );
        if let Some((own_id, _, last_sequence)) = &allocator {
            anyhow::ensure!(
                origin != own_id || entry.position.sequence <= *last_sequence,
                "unallocated operation claims local origin"
            );
        }
        if let Some(previous) = identities.insert(entry.op.id.as_str(), entry) {
            anyhow::ensure!(previous == entry, "conflicting operation UUID in batch");
        }
        if let Some(previous) = positions.insert((origin, entry.position.sequence), entry) {
            anyhow::ensure!(previous == entry, "conflicting origin sequence in batch");
        }
        if let Some(retained) = sequenced_op(store, &entry.op)? {
            anyhow::ensure!(
                retained.position == entry.position,
                "operation UUID reused at another sequence"
            );
        }
        if let Some(bytes) = store.get(&super::op_log_store_key(entry.op.id.as_str()))? {
            let (_, retained) = super::parse_durable_op_log_value(&bytes)?;
            anyhow::ensure!(
                retained == entry.op,
                "operation UUID reused with another payload"
            );
            anyhow::ensure!(
                store.get(&position_key(entry.op.id.as_str()))?.is_some(),
                "operation UUID conflicts with legacy replay"
            );
        }
        if entry.position.sequence > progress.contiguous
            && !progress.sparse.contains_key(&entry.position.sequence)
        {
            anyhow::ensure!(
                store
                    .get(&super::seen_op_store_key(entry.op.id.as_str()))?
                    .is_none(),
                "operation UUID reused at another sequence"
            );
        }
    }
    let mut accepted = Vec::new();
    let mut ordered = operations.iter().collect::<Vec<_>>();
    ordered.sort_by_key(|entry| (entry.op.origin.as_str(), entry.position.sequence));
    for entry in ordered {
        anyhow::ensure!(
            serde_json::to_vec(entry)?.len() + 1024 <= REPLAY_BYTES,
            "operation exceeds replay byte budget"
        );
        let origin = entry.op.origin.as_str();
        if let Some((own_id, _, last_sequence)) = &allocator {
            anyhow::ensure!(
                origin != own_id || entry.position.sequence <= *last_sequence,
                "unallocated operation claims local origin"
            );
        }
        if !updates.contains_key(origin) {
            updates.insert(origin.to_owned(), tracker(store, origin)?);
        }
        let progress = updates
            .get_mut(origin)
            .ok_or_else(|| anyhow::anyhow!("missing planned origin"))?;
        if progress.observe(entry.position, entry.op.id.as_str())? {
            accepted.push(entry.clone());
        }
    }
    let mut sets = tracker_sets(store, updates)?;
    for entry in &accepted {
        sets.push((
            position_key(entry.op.id.as_str()),
            serde_json::to_vec(&(
                entry.op.origin.as_str(),
                entry.position.generation,
                entry.position.sequence,
            ))?,
        ));
    }
    Ok((accepted, sets))
}

pub(in crate::sync_manager) fn summary_page(
    store: &NxStore,
    after: Option<&[u8]>,
) -> anyhow::Result<(Vec<HistorySummary>, Option<Vec<u8>>)> {
    let page = store.scan_prefix_page_after_bounded(
        ORIGINS,
        after,
        SUMMARY_PAGE as u32,
        MAX_TRACKING_BYTES,
    )?;
    let cursor = page.last().map(|(key, _)| key.clone());
    let mut entries = Vec::new();
    for (key, bytes) in page {
        let origin = std::str::from_utf8(&key[ORIGINS.len()..])?;
        origin_key(origin)?;
        entries.push(HistorySummary {
            baseline_required: ReceiptTracker::decode(&bytes)?.baseline_required,
            origin: NodeId::new(origin),
            high_sequence: ReceiptTracker::decode(&bytes)?.high,
        });
    }
    Ok((entries, cursor))
}

pub(in crate::sync_manager) fn record_summary(
    store: &NxStore,
    summary: &HistorySummary,
) -> anyhow::Result<Option<(u64, u64)>> {
    let lease = store.acquire_write_lease()?;
    let origin = summary.origin.as_str();
    if let Some((own, _, last)) = local(store)? {
        anyhow::ensure!(
            origin != own || summary.high_sequence <= last,
            "summary claims unallocated local history"
        );
    }
    let mut progress = tracker(store, origin)?;
    progress.high = progress.high.max(summary.high_sequence);
    progress.baseline_required |= summary.baseline_required;
    let missing = progress.missing_page();
    let sets = tracker_sets(store, BTreeMap::from([(origin.to_owned(), progress)]))?;
    let refs = sets
        .iter()
        .map(|(k, v)| (k.as_slice(), v.as_slice()))
        .collect::<Vec<_>>();
    lease.apply_batch(&refs, &[])?;
    Ok(missing)
}

pub(in crate::sync_manager) fn record_unavailable(
    store: &NxStore,
    origin: &NodeId,
    evidence: UnavailableHistory,
) -> anyhow::Result<()> {
    let lease = store.acquire_write_lease()?;
    let mut progress = tracker(store, origin.as_str())?;
    if evidence.end > progress.high || !progress.has_missing(evidence.start, evidence.end) {
        return Ok(());
    }
    progress.unavailable = Some(evidence);
    ReceiptTracker::decode(&progress.encode()?)?;
    let sets = tracker_sets(
        store,
        BTreeMap::from([(origin.as_str().to_owned(), progress)]),
    )?;
    let refs = sets
        .iter()
        .map(|(k, v)| (k.as_slice(), v.as_slice()))
        .collect::<Vec<_>>();
    lease.apply_batch(&refs, &[])?;
    Ok(())
}

pub(in crate::sync_manager) fn missing_range(
    store: &NxStore,
    origin: &NodeId,
) -> anyhow::Result<Option<(u64, u64)>> {
    Ok(tracker(store, origin.as_str())?.missing_page())
}

pub(in crate::sync_manager) fn replication_progress(
    store: &NxStore,
) -> anyhow::Result<Vec<ReplicationProgress>> {
    let mut after = None;
    let mut result = Vec::new();
    loop {
        let page = store.scan_prefix_page_after_bounded(
            ORIGINS,
            after.as_deref(),
            SUMMARY_PAGE as u32,
            MAX_TRACKING_BYTES,
        )?;
        if page.is_empty() {
            break;
        }
        for (key, bytes) in &page {
            let origin = std::str::from_utf8(&key[ORIGINS.len()..])?;
            let progress = ReceiptTracker::decode(bytes)?;
            result.push(ReplicationProgress {
                origin: NodeId::new(origin),
                contiguous_sequence: progress.contiguous,
                known_high_sequence: progress.high,
                sparse_positions: progress.sparse.len(),
                source_unavailable: progress.unavailable.is_some(),
                unavailable_evidence: progress.unavailable,
                baseline_required: progress.baseline_required,
            });
            anyhow::ensure!(result.len() <= MAX_ORIGINS, "origin budget exceeded");
        }
        after = page.last().map(|(key, _)| key.clone());
    }
    Ok(result)
}

pub(in crate::sync_manager) fn validate_sequence_record(
    key: &[u8],
    value: &[u8],
) -> anyhow::Result<()> {
    if key == LOCAL || key.starts_with(POSITIONS) {
        let (origin, generation, _): LocalAllocator = serde_json::from_slice(value)?;
        origin_key(&origin)?;
        anyhow::ensure!(generation > 0, "zero stored generation");
    } else if key == USAGE {
        let (origins, bytes, sparse): Usage = serde_json::from_slice(value)?;
        anyhow::ensure!(
            origins <= MAX_ORIGINS && bytes <= MAX_TRACKING_BYTES && sparse <= MAX_TOTAL_SPARSE,
            "invalid tracking usage"
        );
    } else if key.starts_with(ORIGINS) {
        origin_key(std::str::from_utf8(&key[ORIGINS.len()..])?)?;
        ReceiptTracker::decode(value)?;
    } else {
        anyhow::bail!("unknown sequence metadata record");
    }
    Ok(())
}

pub(in crate::sync_manager) fn summary_cursor(
    origin: Option<&str>,
) -> anyhow::Result<Option<Vec<u8>>> {
    origin.map(origin_key).transpose()
}

pub(in crate::sync_manager) fn migrate_sequence_v1(
    key: &[u8],
    value: &[u8],
) -> anyhow::Result<Option<Vec<u8>>> {
    if key.starts_with(ORIGINS) {
        origin_key(std::str::from_utf8(&key[ORIGINS.len()..])?)?;
        let (contiguous, high, sparse, _unavailable, baseline_required): (
            u64,
            u64,
            BTreeMap<u64, (u64, String)>,
            bool,
            bool,
        ) = serde_json::from_slice(value)?;
        let progress = ReceiptTracker {
            contiguous,
            high,
            sparse,
            unavailable: None,
            baseline_required,
        };
        let encoded = progress.encode()?;
        ReceiptTracker::decode(&encoded)?;
        Ok(Some(encoded))
    } else {
        validate_sequence_record(key, value)?;
        Ok(None)
    }
}

pub(in crate::sync_manager) fn migrated_sequence_usage(
    store: &NxStore,
) -> anyhow::Result<(Vec<u8>, Vec<u8>)> {
    let mut actual = (0usize, 0usize, 0usize);
    let mut cursor = None;
    loop {
        let page = store.scan_prefix_page_after_bounded(
            ORIGINS,
            cursor.as_deref(),
            SUMMARY_PAGE as u32,
            MAX_TRACKING_BYTES,
        )?;
        if page.is_empty() {
            break;
        }
        for (key, value) in &page {
            let progress = ReceiptTracker::decode(value)?;
            actual.0 += 1;
            actual.1 += key.len() + value.len();
            actual.2 += progress.sparse.len();
            anyhow::ensure!(
                actual.0 <= MAX_ORIGINS
                    && actual.1 <= MAX_TRACKING_BYTES
                    && actual.2 <= MAX_TOTAL_SPARSE,
                "migrated history exceeds tracking budget"
            );
        }
        cursor = page.last().map(|(key, _)| key.clone());
    }
    let previous = usage(store)?;
    anyhow::ensure!(
        actual.0 == previous.0 && actual.2 == previous.2,
        "history tracking accounting mismatch"
    );
    Ok((USAGE.to_vec(), serde_json::to_vec(&actual)?))
}
