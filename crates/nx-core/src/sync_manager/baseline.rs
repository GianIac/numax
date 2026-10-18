//! Bounded full-state recovery, separate from operation replay and transport ownership.
//! A dominating complete cut is required: max-merging partial additive counters is unsafe.
use std::collections::HashMap;
use std::io::Write;

use nx_sync::{
    BaselineFailure, BaselineRecord, CrdtBaseline, GCounter, LwwMap, LwwRegister, ORSet, PNCounter,
    Rga,
};

use super::storage::*;
use super::types::*;

pub(super) const VERSION: u16 = 1;
pub(super) const MAX_BYTES: usize = 1024 * 1024;
const MAX_RECORDS: usize = 4096;
const MAX_KEY_BYTES: usize = 4096;

#[derive(Debug)]
pub(super) struct Rejected(pub BaselineFailure);
impl std::fmt::Display for Rejected {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "baseline rejected: {:?}", self.0)
    }
}
impl std::error::Error for Rejected {}

struct BoundedBytes(Vec<u8>);
impl Write for BoundedBytes {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > MAX_BYTES.saturating_sub(self.0.len()) {
            return Err(std::io::Error::other("baseline byte budget exceeded"));
        }
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// All async locks precede the synchronous write lease, matching apply/local writes.
pub(super) async fn export(context: &RemoteOpApplyContext<'_>) -> anyhow::Result<Vec<u8>> {
    let _seen = context.seen_ops.read().await;
    let _log = context.op_log.read().await;
    let counters = context.counters.read().await;
    let pncounters = context.pncounters.read().await;
    let registers = context.lww_registers.read().await;
    let maps = context.lww_maps.read().await;
    let sets = context.orsets.read().await;
    let rgas = context.rgas.read().await;
    let lease = context.store.acquire_write_lease()?;
    let origins = baseline_origins(lease.store())?;
    let mut baseline = CrdtBaseline {
        origins,
        records: Vec::new(),
    };
    let mut state_bytes = 0usize;
    macro_rules! collect {
        ($family:expr, $registry:expr) => {
            for (key, state) in $registry.iter() {
                if baseline.records.len() >= MAX_RECORDS || key.len() > MAX_KEY_BYTES {
                    return Err(Rejected(BaselineFailure::BudgetExceeded).into());
                }
                let mut encoded = BoundedBytes(Vec::new());
                serde_json::to_writer(&mut encoded, state)
                    .map_err(|_| Rejected(BaselineFailure::BudgetExceeded))?;
                state_bytes += key.len() + encoded.0.len();
                if state_bytes > MAX_BYTES {
                    return Err(Rejected(BaselineFailure::BudgetExceeded).into());
                }
                baseline.records.push(BaselineRecord {
                    family: $family,
                    key: key.clone(),
                    state: encoded.0,
                });
            }
        };
    }
    collect!(1, counters);
    collect!(2, pncounters);
    collect!(3, registers);
    collect!(4, maps);
    collect!(5, sets);
    collect!(6, rgas);
    baseline
        .records
        .sort_by(|a, b| (a.family, &a.key).cmp(&(b.family, &b.key)));
    let mut payload = BoundedBytes(Vec::new());
    serde_json::to_writer(&mut payload, &baseline)
        .map_err(|_| Rejected(BaselineFailure::BudgetExceeded))?;
    // Freeze the cut only after a covering flush. This is not a remote write acknowledgement.
    lease.flush()?;
    Ok(payload.0)
}

#[derive(Default)]
struct Decoded {
    counters: HashMap<String, GCounter>,
    pncounters: HashMap<String, PNCounter>,
    registers: HashMap<String, LwwRegister>,
    maps: HashMap<String, LwwMap>,
    sets: HashMap<String, ORSet>,
    rgas: HashMap<String, Rga>,
    writes: Vec<(Vec<u8>, Vec<u8>)>,
}

/// Validate counter totals before invoking value(), which assumes a representable u64 sum.
fn validate_counter(value: &serde_json::Value) -> anyhow::Result<()> {
    let counts = value
        .get("counts")
        .and_then(|v| v.as_object())
        .ok_or_else(|| anyhow::anyhow!("missing counter slots"))?;
    let mut sum = 0u64;
    for (origin, slot) in counts {
        anyhow::ensure!(
            !origin.is_empty() && origin.len() <= 256,
            "invalid counter origin"
        );
        sum = sum
            .checked_add(
                slot.as_u64()
                    .ok_or_else(|| anyhow::anyhow!("invalid counter slot"))?,
            )
            .ok_or_else(|| anyhow::anyhow!("counter total overflow"))?;
    }
    Ok(())
}

fn validate_rga(value: &serde_json::Value) -> anyhow::Result<()> {
    let elements = value
        .get("elements")
        .and_then(|v| v.as_object())
        .ok_or_else(|| anyhow::anyhow!("missing RGA elements"))?;
    anyhow::ensure!(elements.len() <= MAX_RECORDS, "RGA element budget exceeded");
    for (id, element) in elements {
        anyhow::ensure!(
            element.get("id").and_then(|v| v.as_str()) == Some(id.as_str()),
            "RGA element identity mismatch"
        );
        let mut current = Some(id.as_str());
        // Bound recursion of the existing materialization implementation and reject cycles.
        let mut depth = 0;
        while let Some(entry) = current.and_then(|key| elements.get(key)) {
            depth += 1;
            anyhow::ensure!(depth <= 128, "RGA baseline depth exceeded or cycle");
            current = entry.get("parent").and_then(|v| v.as_str());
        }
    }
    Ok(())
}

impl Decoded {
    fn new(baseline: &CrdtBaseline) -> anyhow::Result<Self> {
        anyhow::ensure!(
            baseline.records.len() <= MAX_RECORDS,
            "baseline record budget exceeded"
        );
        anyhow::ensure!(
            baseline
                .records
                .windows(2)
                .all(|p| (p[0].family, &p[0].key) < (p[1].family, &p[1].key)),
            "duplicate or unordered baseline records"
        );
        let mut decoded = Self::default();
        for record in &baseline.records {
            anyhow::ensure!(
                record.key.len() <= MAX_KEY_BYTES,
                "baseline key budget exceeded"
            );
            let value: serde_json::Value = serde_json::from_slice(&record.state)?;
            match record.family {
                1 => validate_counter(&value)?,
                2 => {
                    validate_counter(&value["positive"])?;
                    validate_counter(&value["negative"])?;
                }
                6 => validate_rga(&value)?,
                _ => {}
            }
            macro_rules! decode {
                ($kind:ident, $parse:ident, $registry:ident, $materialize:expr) => {{
                    let state = $parse(&record.state)?;
                    let materialized = ($materialize)(&state)?;
                    decoded.writes.push((
                        crdt_store_key(CrdtKind::$kind, CrdtStoreNamespace::State, &record.key),
                        record.state.clone(),
                    ));
                    decoded.writes.push((
                        crdt_store_key(
                            CrdtKind::$kind,
                            CrdtStoreNamespace::Materialized,
                            &record.key,
                        ),
                        materialized,
                    ));
                    decoded.$registry.insert(record.key.clone(), state);
                }};
            }
            match record.family {
                1 => decode!(
                    GCounter,
                    parse_durable_gcounter_state,
                    counters,
                    |s: &GCounter| Ok::<_, anyhow::Error>(s.value().to_le_bytes().to_vec())
                ),
                2 => decode!(
                    PNCounter,
                    parse_durable_pncounter_state,
                    pncounters,
                    |s: &PNCounter| Ok::<_, anyhow::Error>(s.value().to_le_bytes().to_vec())
                ),
                3 => decode!(
                    LwwRegister,
                    parse_durable_lww_register_state,
                    registers,
                    |s: &LwwRegister| Ok::<_, anyhow::Error>(s.value_bytes())
                ),
                4 => decode!(LwwMap, parse_durable_lww_map_state, maps, |s: &LwwMap| {
                    serde_json::to_vec(&s.entries())
                }),
                5 => decode!(ORSet, parse_durable_orset_state, sets, |s: &ORSet| {
                    serde_json::to_vec(&s.elements())
                }),
                6 => decode!(Rga, parse_durable_rga_state, rgas, |s: &Rga| {
                    serde_json::to_vec(&s.values())
                }),
                _ => anyhow::bail!("unknown baseline CRDT family"),
            }
        }
        Ok(decoded)
    }
}

pub(super) async fn import(
    version: u16,
    payload: &[u8],
    digest: &[u8; 32],
    required_origin: &nx_sync::NodeId,
    context: &RemoteOpApplyContext<'_>,
) -> anyhow::Result<()> {
    if version != VERSION {
        return Err(Rejected(BaselineFailure::UnsupportedVersion).into());
    }
    if payload.len() > MAX_BYTES {
        return Err(Rejected(BaselineFailure::BudgetExceeded).into());
    }
    let baseline = decode(payload, digest, required_origin)?;
    let decoded = Decoded::new(&baseline)
        .map_err(|error| error.context(Rejected(BaselineFailure::InvalidState)))?;
    install(baseline, decoded, context).await
}

fn decode(
    payload: &[u8],
    digest: &[u8; 32],
    required_origin: &nx_sync::NodeId,
) -> anyhow::Result<CrdtBaseline> {
    let result = (|| {
        anyhow::ensure!(
            blake3::hash(payload).as_bytes() == digest,
            "baseline digest mismatch"
        );
        let baseline: CrdtBaseline = serde_json::from_slice(payload)?;
        anyhow::ensure!(
            baseline
                .origins
                .iter()
                .any(|o| &o.origin == required_origin),
            "baseline omits requested origin"
        );
        Ok(baseline)
    })();
    result.map_err(|error: anyhow::Error| error.context(Rejected(BaselineFailure::InvalidState)))
}

async fn install(
    baseline: CrdtBaseline,
    decoded: Decoded,
    context: &RemoteOpApplyContext<'_>,
) -> anyhow::Result<()> {
    let _seen = context.seen_ops.write().await;
    let _log = context.op_log.write().await;
    let mut counters = context.counters.write().await;
    let mut pncounters = context.pncounters.write().await;
    let mut registers = context.lww_registers.write().await;
    let mut maps = context.lww_maps.write().await;
    let mut sets = context.orsets.write().await;
    let mut rgas = context.rgas.write().await;
    let lease = context.store.acquire_write_lease()?;
    let sequence_sets = baseline_sequence_sets(&lease, &baseline.origins).map_err(|error| {
        if error
            .downcast_ref::<super::sequence::TrackingLimit>()
            .is_some()
        {
            error.context(Rejected(BaselineFailure::BudgetExceeded))
        } else {
            error
        }
    })?;
    // Also check state inclusion: a cut alone cannot justify silently dropping an existing key,
    // a tombstone, or state from a legacy store. Compare canonical JSON values for counters too.
    macro_rules! check_inclusion {
        ($old:expr, $new:expr) => {
            for (key, state) in $old.iter() {
                let offered = $new
                    .get(key)
                    .ok_or(Rejected(BaselineFailure::CoverageConflict))?;
                let merged = offered.merged_with(state);
                if serde_json::to_value(&merged)? != serde_json::to_value(offered)? {
                    return Err(Rejected(BaselineFailure::CoverageConflict).into());
                }
            }
        };
    }
    check_inclusion!(counters, decoded.counters);
    check_inclusion!(pncounters, decoded.pncounters);
    check_inclusion!(registers, decoded.registers);
    check_inclusion!(maps, decoded.maps);
    check_inclusion!(sets, decoded.sets);
    check_inclusion!(rgas, decoded.rgas);
    let writes = decoded
        .writes
        .iter()
        .chain(sequence_sets.iter())
        .map(|(k, v)| (k.as_slice(), v.as_slice()))
        .collect::<Vec<_>>();
    lease.apply_batch(&writes, &[])?;
    // A flush failure is an uncertain outcome: the whole batch may already survive.
    // Publish that accepted batch coherently in memory, but retain the confirmation marker
    // and return an error until a retry/restart confirms a covering flush.
    let flushed = lease.flush();
    *counters = decoded.counters;
    *pncounters = decoded.pncounters;
    *registers = decoded.registers;
    *maps = decoded.maps;
    *sets = decoded.sets;
    *rgas = decoded.rgas;
    flushed?;
    lease.delete(BASELINE_UNCONFIRMED)?;
    Ok(())
}
