use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::num::{NonZeroU32, NonZeroUsize};

use nx_store::{Store as NxStore, StoreWriteLease};

use super::schema::{SCHEMA_KEY_PREFIX, SchemaError, SchemaHeader, StoreTable, validate_header};
use super::storage::{
    logical_key_for_prefix, parse_durable_gcounter_state, parse_durable_lww_map_state,
    parse_durable_lww_register_state, parse_durable_op_log_value, parse_durable_orset_state,
    parse_durable_pncounter_state, parse_durable_rga_state, parse_materialized_gcounter_value,
    parse_materialized_pncounter_value, parse_seen_op_sequence,
};

const MIGRATION_MAGIC: [u8; 4] = *b"NXMG";
const CHECKPOINT_FORMAT_VERSION: u16 = 1;
const CHECKPOINT_FIXED_LEN: usize = 16;
const MIGRATION_KEY_PREFIX: &str = "__nx/migration/";
pub const DEFAULT_MIGRATION_BATCH_SIZE: NonZeroU32 =
    NonZeroU32::new(512).expect("migration batch size must be non-zero");
pub const DEFAULT_MIGRATION_BATCH_BYTES: NonZeroUsize =
    NonZeroUsize::new(4 * 1024 * 1024).expect("migration byte limit must be non-zero");

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MigrationOptions {
    pub max_records: NonZeroU32,
    pub max_bytes: NonZeroUsize,
}

impl Default for MigrationOptions {
    fn default() -> Self {
        Self {
            max_records: DEFAULT_MIGRATION_BATCH_SIZE,
            max_bytes: DEFAULT_MIGRATION_BATCH_BYTES,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MigrationProgress {
    BatchValidated {
        table: &'static str,
        records: usize,
    },
    TableCompleted {
        table: &'static str,
        from_version: u16,
        to_version: u16,
    },
    Complete,
}

pub struct SyncSchemaMigration<'a> {
    store: StoreWriteLease<'a>,
    options: MigrationOptions,
}

#[derive(Debug)]
pub enum MigrationError {
    Store(nx_store::StoreError),
    Schema(String),
    InvalidCheckpoint {
        table: &'static str,
        reason: &'static str,
    },
    CheckpointTableMismatch {
        expected: &'static str,
        actual: u16,
    },
    UnsupportedPath {
        table: &'static str,
        from_version: u16,
        to_version: u16,
    },
    InvalidRecord {
        table: &'static str,
        key: Vec<u8>,
        reason: String,
    },
    StaleCheckpoint {
        table: &'static str,
    },
    InvalidRegistry {
        table: &'static str,
        reason: &'static str,
    },
    BatchMemoryLimitExceeded {
        table: &'static str,
        bytes: usize,
        max_bytes: usize,
    },
    InvalidMutation {
        table: &'static str,
        key: Vec<u8>,
        reason: &'static str,
    },
}

impl fmt::Display for MigrationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Store(error) => write!(formatter, "migration store error: {error}"),
            Self::Schema(error) => write!(formatter, "{error}"),
            Self::InvalidCheckpoint { table, reason } => {
                write!(
                    formatter,
                    "invalid migration checkpoint for {table}: {reason}"
                )
            }
            Self::CheckpointTableMismatch { expected, actual } => write!(
                formatter,
                "migration checkpoint table mismatch: expected {expected}, got table id {actual}"
            ),
            Self::UnsupportedPath {
                table,
                from_version,
                to_version,
            } => write!(
                formatter,
                "unsupported migration path for {table}: {from_version} -> {to_version}"
            ),
            Self::InvalidRecord { table, key, reason } => write!(
                formatter,
                "invalid legacy record in {table} at key {:?}: {reason}",
                String::from_utf8_lossy(key)
            ),
            Self::StaleCheckpoint { table } => {
                write!(formatter, "stale migration checkpoint found for {table}")
            }
            Self::InvalidRegistry { table, reason } => {
                write!(
                    formatter,
                    "invalid migration registry for {table}: {reason}"
                )
            }
            Self::BatchMemoryLimitExceeded {
                table,
                bytes,
                max_bytes,
            } => write!(
                formatter,
                "migration batch for {table} uses {bytes} bytes, exceeding limit {max_bytes}"
            ),
            Self::InvalidMutation { table, key, reason } => write!(
                formatter,
                "invalid migration mutation for {table} at key {:?}: {reason}",
                String::from_utf8_lossy(key)
            ),
        }
    }
}

impl std::error::Error for MigrationError {}

impl From<nx_store::StoreError> for MigrationError {
    fn from(error: nx_store::StoreError) -> Self {
        Self::Store(error)
    }
}

impl From<SchemaError> for MigrationError {
    fn from(error: SchemaError) -> Self {
        Self::Schema(error.to_string())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MigrationCheckpoint {
    table: StoreTable,
    from_version: u16,
    to_version: u16,
    last_processed_key: Option<Vec<u8>>,
}

#[derive(Clone, Copy)]
struct MigrationStep {
    from_version: u16,
    to_version: u16,
    migrate_record: RecordMigrator,
}

#[derive(Debug, Default, PartialEq, Eq)]
struct RecordMigration {
    sets: Vec<(Vec<u8>, Vec<u8>)>,
    deletes: Vec<Vec<u8>>,
}

impl RecordMigration {
    fn unchanged() -> Self {
        Self::default()
    }

    #[cfg(test)]
    fn replace_value(key: &[u8], value: Vec<u8>) -> Self {
        Self {
            sets: vec![(key.to_vec(), value)],
            deletes: Vec::new(),
        }
    }

    #[cfg(test)]
    fn delete(key: &[u8]) -> Self {
        Self {
            sets: Vec::new(),
            deletes: vec![key.to_vec()],
        }
    }

    #[cfg(test)]
    fn move_to(source_key: &[u8], target_key: Vec<u8>, value: Vec<u8>) -> Self {
        Self {
            sets: vec![(target_key, value)],
            deletes: vec![source_key.to_vec()],
        }
    }

    #[cfg(test)]
    fn with_set(mut self, key: Vec<u8>, value: Vec<u8>) -> Self {
        self.sets.push((key, value));
        self
    }
}

type RecordMigrator = fn(StoreTable, &[u8], &[u8]) -> Result<RecordMigration, MigrationError>;

const LEGACY_TO_V1: MigrationStep = MigrationStep {
    from_version: 0,
    to_version: 1,
    migrate_record: migrate_legacy_record,
};

const GCOUNTER_MATERIALIZED_STEPS: &[MigrationStep] = &[LEGACY_TO_V1];
const GCOUNTER_STATE_STEPS: &[MigrationStep] = &[LEGACY_TO_V1];
const PNCOUNTER_MATERIALIZED_STEPS: &[MigrationStep] = &[LEGACY_TO_V1];
const PNCOUNTER_STATE_STEPS: &[MigrationStep] = &[LEGACY_TO_V1];
const LWW_REGISTER_MATERIALIZED_STEPS: &[MigrationStep] = &[LEGACY_TO_V1];
const LWW_REGISTER_STATE_STEPS: &[MigrationStep] = &[LEGACY_TO_V1];
const LWW_MAP_MATERIALIZED_STEPS: &[MigrationStep] = &[LEGACY_TO_V1];
const LWW_MAP_STATE_STEPS: &[MigrationStep] = &[LEGACY_TO_V1];
const ORSET_MATERIALIZED_STEPS: &[MigrationStep] = &[LEGACY_TO_V1];
const ORSET_STATE_STEPS: &[MigrationStep] = &[LEGACY_TO_V1];
const RGA_MATERIALIZED_STEPS: &[MigrationStep] = &[LEGACY_TO_V1];
const RGA_STATE_STEPS: &[MigrationStep] = &[LEGACY_TO_V1];
const SEEN_OPS_STEPS: &[MigrationStep] = &[LEGACY_TO_V1];
const OP_LOG_STEPS: &[MigrationStep] = &[LEGACY_TO_V1];

impl MigrationCheckpoint {
    fn new(table: StoreTable, from_version: u16, to_version: u16) -> Self {
        Self {
            table,
            from_version,
            to_version,
            last_processed_key: None,
        }
    }

    fn encode(&self) -> Result<Vec<u8>, MigrationError> {
        let key = self.last_processed_key.as_deref().unwrap_or_default();
        let key_len = u32::try_from(key.len()).map_err(|_| MigrationError::InvalidCheckpoint {
            table: self.table.name(),
            reason: "cursor key exceeds u32",
        })?;
        let mut bytes = Vec::with_capacity(CHECKPOINT_FIXED_LEN + key.len());
        bytes.extend_from_slice(&MIGRATION_MAGIC);
        bytes.extend_from_slice(&CHECKPOINT_FORMAT_VERSION.to_be_bytes());
        bytes.extend_from_slice(&(self.table as u16).to_be_bytes());
        bytes.extend_from_slice(&self.from_version.to_be_bytes());
        bytes.extend_from_slice(&self.to_version.to_be_bytes());
        bytes.extend_from_slice(&key_len.to_be_bytes());
        bytes.extend_from_slice(key);
        Ok(bytes)
    }

    fn decode(table: StoreTable, bytes: &[u8]) -> Result<Self, MigrationError> {
        if bytes.len() < CHECKPOINT_FIXED_LEN {
            return Err(MigrationError::InvalidCheckpoint {
                table: table.name(),
                reason: "checkpoint is truncated",
            });
        }
        if bytes[..4] != MIGRATION_MAGIC {
            return Err(MigrationError::InvalidCheckpoint {
                table: table.name(),
                reason: "invalid magic",
            });
        }
        let format_version = u16::from_be_bytes([bytes[4], bytes[5]]);
        if format_version != CHECKPOINT_FORMAT_VERSION {
            return Err(MigrationError::InvalidCheckpoint {
                table: table.name(),
                reason: "unsupported checkpoint format version",
            });
        }
        let actual_table = u16::from_be_bytes([bytes[6], bytes[7]]);
        if actual_table != table as u16 {
            return Err(MigrationError::CheckpointTableMismatch {
                expected: table.name(),
                actual: actual_table,
            });
        }
        let from_version = u16::from_be_bytes([bytes[8], bytes[9]]);
        let to_version = u16::from_be_bytes([bytes[10], bytes[11]]);
        let key_len = u32::from_be_bytes([bytes[12], bytes[13], bytes[14], bytes[15]]) as usize;
        let expected_len =
            CHECKPOINT_FIXED_LEN
                .checked_add(key_len)
                .ok_or(MigrationError::InvalidCheckpoint {
                    table: table.name(),
                    reason: "cursor length overflow",
                })?;
        if bytes.len() != expected_len {
            return Err(MigrationError::InvalidCheckpoint {
                table: table.name(),
                reason: "cursor length does not match checkpoint",
            });
        }

        Ok(Self {
            table,
            from_version,
            to_version,
            last_processed_key: (key_len > 0).then(|| bytes[CHECKPOINT_FIXED_LEN..].to_vec()),
        })
    }
}

pub fn migrate_sync_schema(
    store: &NxStore,
    options: MigrationOptions,
) -> Result<(), MigrationError> {
    let mut migration = SyncSchemaMigration::new(store, options)?;
    loop {
        if matches!(migration.step()?, MigrationProgress::Complete) {
            return Ok(());
        }
    }
}

impl<'a> SyncSchemaMigration<'a> {
    pub fn new(store: &'a NxStore, options: MigrationOptions) -> Result<Self, MigrationError> {
        validate_migration_registries()?;
        Ok(Self {
            store: store.acquire_write_lease()?,
            options,
        })
    }

    pub fn step(&mut self) -> Result<MigrationProgress, MigrationError> {
        migrate_sync_schema_batch(&self.store, self.options)
    }
}

fn migrate_sync_schema_batch(
    lease: &StoreWriteLease<'_>,
    options: MigrationOptions,
) -> Result<MigrationProgress, MigrationError> {
    let store = lease.store();
    preflight_checkpoints(store)?;

    for table in StoreTable::ALL {
        if let Some(progress) = migrate_table_batch(
            lease,
            table,
            table.current_version(),
            migration_steps(table),
            options,
        )? {
            return Ok(progress);
        }
    }

    Ok(MigrationProgress::Complete)
}

fn migrate_table_batch(
    lease: &StoreWriteLease<'_>,
    table: StoreTable,
    target_version: u16,
    steps: &[MigrationStep],
    options: MigrationOptions,
) -> Result<Option<MigrationProgress>, MigrationError> {
    let store = lease.store();
    let checkpoint_key = checkpoint_key(table);
    let checkpoint_bytes = store.get(&checkpoint_key)?;
    let schema_bytes = store.get(&table.schema_key())?;
    let from_version = match schema_bytes {
        Some(bytes) => {
            let header = SchemaHeader::decode(table, &bytes)?;
            if header.version == target_version {
                if checkpoint_bytes.is_some() {
                    return Err(MigrationError::StaleCheckpoint {
                        table: table.name(),
                    });
                }
                return Ok(None);
            }
            if header.version > target_version {
                validate_header(table, &bytes)?;
                unreachable!("future schema validation must fail");
            }
            header.version
        }
        None => 0,
    };
    let step = next_migration_step(table, from_version, target_version, steps)?;
    let mut checkpoint = match checkpoint_bytes {
        Some(bytes) => MigrationCheckpoint::decode(table, &bytes)?,
        None => MigrationCheckpoint::new(table, step.from_version, step.to_version),
    };
    if checkpoint.from_version != step.from_version || checkpoint.to_version != step.to_version {
        return Err(MigrationError::InvalidCheckpoint {
            table: table.name(),
            reason: "checkpoint migration path does not match registered step",
        });
    }

    let entries = store.scan_prefix_page_after_bounded(
        table.data_prefix().as_bytes(),
        checkpoint.last_processed_key.as_deref(),
        options.max_records.get(),
        options.max_bytes.get(),
    )?;
    if entries.is_empty() {
        let schema_key = table.schema_key();
        let schema_header = SchemaHeader {
            version: step.to_version,
            table,
        }
        .encode();
        lease.apply_batch(
            &[(schema_key.as_slice(), schema_header.as_slice())],
            &[checkpoint_key.as_slice()],
        )?;
        return Ok(Some(MigrationProgress::TableCompleted {
            table: table.name(),
            from_version: step.from_version,
            to_version: step.to_version,
        }));
    }

    let mut batch_bytes = entries.iter().fold(0usize, |total, (key, value)| {
        total.saturating_add(key.len()).saturating_add(value.len())
    });
    let mut sets = BTreeMap::<Vec<u8>, Vec<u8>>::new();
    let mut deletes = BTreeSet::<Vec<u8>>::new();
    for (key, value) in &entries {
        let migration = (step.migrate_record)(table, key, value)?;
        collect_record_migration(
            table,
            key,
            migration,
            &mut sets,
            &mut deletes,
            &mut batch_bytes,
            options.max_bytes.get(),
        )?;
    }
    checkpoint.last_processed_key = entries.last().map(|(key, _)| key.clone());
    sets.insert(checkpoint_key, checkpoint.encode()?);
    let set_entries = sets.into_iter().collect::<Vec<_>>();
    let delete_entries = deletes.into_iter().collect::<Vec<_>>();
    let set_refs = set_entries
        .iter()
        .map(|(key, value)| (key.as_slice(), value.as_slice()))
        .collect::<Vec<_>>();
    let delete_refs = delete_entries.iter().map(Vec::as_slice).collect::<Vec<_>>();
    lease.apply_batch(&set_refs, &delete_refs)?;

    Ok(Some(MigrationProgress::BatchValidated {
        table: table.name(),
        records: entries.len(),
    }))
}

#[allow(clippy::too_many_arguments)]
fn collect_record_migration(
    table: StoreTable,
    source_key: &[u8],
    migration: RecordMigration,
    sets: &mut BTreeMap<Vec<u8>, Vec<u8>>,
    deletes: &mut BTreeSet<Vec<u8>>,
    batch_bytes: &mut usize,
    max_bytes: usize,
) -> Result<(), MigrationError> {
    for (key, value) in migration.sets {
        validate_record_mutation_key(table, source_key, &key)?;
        if deletes.contains(&key) || sets.contains_key(&key) {
            return Err(MigrationError::InvalidMutation {
                table: table.name(),
                key,
                reason: "duplicate or conflicting mutation",
            });
        }
        *batch_bytes = batch_bytes
            .saturating_add(key.len())
            .saturating_add(value.len());
        if *batch_bytes > max_bytes {
            return Err(MigrationError::BatchMemoryLimitExceeded {
                table: table.name(),
                bytes: *batch_bytes,
                max_bytes,
            });
        }
        sets.insert(key, value);
    }
    for key in migration.deletes {
        validate_record_mutation_key(table, source_key, &key)?;
        if sets.contains_key(&key) || !deletes.insert(key.clone()) {
            return Err(MigrationError::InvalidMutation {
                table: table.name(),
                key,
                reason: "duplicate or conflicting mutation",
            });
        }
        *batch_bytes = batch_bytes.saturating_add(key.len());
        if *batch_bytes > max_bytes {
            return Err(MigrationError::BatchMemoryLimitExceeded {
                table: table.name(),
                bytes: *batch_bytes,
                max_bytes,
            });
        }
    }
    Ok(())
}

fn validate_record_mutation_key(
    table: StoreTable,
    source_key: &[u8],
    key: &[u8],
) -> Result<(), MigrationError> {
    validate_mutation_key(table, key)?;
    if key.starts_with(table.data_prefix().as_bytes()) && key != source_key {
        return Err(MigrationError::InvalidMutation {
            table: table.name(),
            key: key.to_vec(),
            reason: "record migrations can only mutate their source key inside the source namespace",
        });
    }
    Ok(())
}

fn validate_mutation_key(table: StoreTable, key: &[u8]) -> Result<(), MigrationError> {
    if key.starts_with(SCHEMA_KEY_PREFIX.as_bytes())
        || key.starts_with(MIGRATION_KEY_PREFIX.as_bytes())
    {
        return Err(MigrationError::InvalidMutation {
            table: table.name(),
            key: key.to_vec(),
            reason: "schema and migration metadata are managed by the engine",
        });
    }
    Ok(())
}

fn migration_steps(table: StoreTable) -> &'static [MigrationStep] {
    match table {
        StoreTable::GCounterMaterialized => GCOUNTER_MATERIALIZED_STEPS,
        StoreTable::GCounterState => GCOUNTER_STATE_STEPS,
        StoreTable::PNCounterMaterialized => PNCOUNTER_MATERIALIZED_STEPS,
        StoreTable::PNCounterState => PNCOUNTER_STATE_STEPS,
        StoreTable::LwwRegisterMaterialized => LWW_REGISTER_MATERIALIZED_STEPS,
        StoreTable::LwwRegisterState => LWW_REGISTER_STATE_STEPS,
        StoreTable::LwwMapMaterialized => LWW_MAP_MATERIALIZED_STEPS,
        StoreTable::LwwMapState => LWW_MAP_STATE_STEPS,
        StoreTable::ORSetMaterialized => ORSET_MATERIALIZED_STEPS,
        StoreTable::ORSetState => ORSET_STATE_STEPS,
        StoreTable::RgaMaterialized => RGA_MATERIALIZED_STEPS,
        StoreTable::RgaState => RGA_STATE_STEPS,
        StoreTable::SeenOps => SEEN_OPS_STEPS,
        StoreTable::OpLog => OP_LOG_STEPS,
    }
}

fn validate_migration_registries() -> Result<(), MigrationError> {
    for table in StoreTable::ALL {
        validate_migration_path(table, table.current_version(), migration_steps(table))?;
    }
    Ok(())
}

fn validate_migration_path(
    table: StoreTable,
    target_version: u16,
    steps: &[MigrationStep],
) -> Result<(), MigrationError> {
    let mut expected_from = 0u16;
    for step in steps {
        if expected_from == target_version {
            return Err(MigrationError::InvalidRegistry {
                table: table.name(),
                reason: "registry contains steps beyond the current schema",
            });
        }
        if step.from_version != expected_from {
            return Err(MigrationError::InvalidRegistry {
                table: table.name(),
                reason: "registry contains a gap or unordered step",
            });
        }
        if step.to_version != expected_from.saturating_add(1) {
            return Err(MigrationError::InvalidRegistry {
                table: table.name(),
                reason: "steps must advance exactly one schema version",
            });
        }
        expected_from = step.to_version;
    }
    if expected_from != target_version {
        return Err(MigrationError::InvalidRegistry {
            table: table.name(),
            reason: "registry does not reach the current schema",
        });
    }
    Ok(())
}

fn next_migration_step(
    table: StoreTable,
    from_version: u16,
    target_version: u16,
    steps: &[MigrationStep],
) -> Result<MigrationStep, MigrationError> {
    let Some(step) = steps
        .iter()
        .find(|step| step.from_version == from_version)
        .copied()
    else {
        return Err(MigrationError::UnsupportedPath {
            table: table.name(),
            from_version,
            to_version: target_version,
        });
    };
    if step.to_version != from_version.saturating_add(1) || step.to_version > target_version {
        return Err(MigrationError::UnsupportedPath {
            table: table.name(),
            from_version,
            to_version: target_version,
        });
    }
    Ok(step)
}

fn preflight_checkpoints(store: &NxStore) -> Result<(), MigrationError> {
    for table in StoreTable::ALL {
        let Some(bytes) = store.get(&checkpoint_key(table))? else {
            continue;
        };
        let checkpoint = MigrationCheckpoint::decode(table, &bytes)?;
        if checkpoint
            .last_processed_key
            .as_deref()
            .is_some_and(|key| !key.starts_with(table.data_prefix().as_bytes()))
        {
            return Err(MigrationError::InvalidCheckpoint {
                table: table.name(),
                reason: "cursor is outside the table prefix",
            });
        }
        let schema_bytes = store.get(&table.schema_key())?;
        let schema_version = match schema_bytes {
            Some(bytes) => SchemaHeader::decode(table, &bytes)?.version,
            None => 0,
        };
        if schema_version == table.current_version() {
            return Err(MigrationError::StaleCheckpoint {
                table: table.name(),
            });
        }
        let step = next_migration_step(
            table,
            schema_version,
            table.current_version(),
            migration_steps(table),
        )?;
        if checkpoint.from_version != step.from_version || checkpoint.to_version != step.to_version
        {
            return Err(MigrationError::InvalidCheckpoint {
                table: table.name(),
                reason: "checkpoint migration path does not match registered step",
            });
        }
    }
    Ok(())
}

fn checkpoint_key(table: StoreTable) -> Vec<u8> {
    format!("{MIGRATION_KEY_PREFIX}{}", table.name()).into_bytes()
}

fn migrate_legacy_record(
    table: StoreTable,
    key: &[u8],
    value: &[u8],
) -> Result<RecordMigration, MigrationError> {
    logical_key_for_prefix(key, table.data_prefix(), table.name())
        .and_then(|_| match table {
            StoreTable::GCounterMaterialized => {
                parse_materialized_gcounter_value(value).map(|_| ())
            }
            StoreTable::GCounterState => parse_durable_gcounter_state(value).map(|_| ()),
            StoreTable::PNCounterMaterialized => {
                parse_materialized_pncounter_value(value).map(|_| ())
            }
            StoreTable::PNCounterState => parse_durable_pncounter_state(value).map(|_| ()),
            StoreTable::LwwRegisterMaterialized => Ok(()),
            StoreTable::LwwRegisterState => parse_durable_lww_register_state(value).map(|_| ()),
            StoreTable::LwwMapMaterialized => {
                serde_json::from_slice::<Vec<(String, Vec<u8>)>>(value)
                    .map(|_| ())
                    .map_err(Into::into)
            }
            StoreTable::LwwMapState => parse_durable_lww_map_state(value).map(|_| ()),
            StoreTable::ORSetMaterialized => serde_json::from_slice::<Vec<String>>(value)
                .map(|_| ())
                .map_err(Into::into),
            StoreTable::ORSetState => parse_durable_orset_state(value).map(|_| ()),
            StoreTable::RgaMaterialized => serde_json::from_slice::<Vec<Vec<u8>>>(value)
                .map(|_| ())
                .map_err(Into::into),
            StoreTable::RgaState => parse_durable_rga_state(value).map(|_| ()),
            StoreTable::SeenOps => parse_seen_op_sequence(value).map(|_| ()),
            StoreTable::OpLog => parse_durable_op_log_value(value).and_then(|(_, op)| {
                let key_op_id = logical_key_for_prefix(key, table.data_prefix(), table.name())?;
                if op.id.as_str() != key_op_id {
                    anyhow::bail!(
                        "op id mismatch: key contains {key_op_id}, value contains {}",
                        op.id
                    );
                }
                Ok(())
            }),
        })
        .map_err(|error| MigrationError::InvalidRecord {
            table: table.name(),
            key: key.to_vec(),
            reason: error.to_string(),
        })?;
    Ok(RecordMigration::unchanged())
}

// End of main code. Test below:

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::mpsc;
    use std::time::Duration;

    use nx_sync::{GCounter, NodeId, Op};

    use super::*;
    use crate::sync_manager::storage::{
        durable_gcounter_state_key, encode_durable_op_log_value, materialized_gcounter_key,
        op_log_store_key,
    };

    fn temp_store() -> Arc<NxStore> {
        Arc::new(NxStore::open(tempfile::tempdir().unwrap().keep()).unwrap())
    }

    fn options(max_records: u32) -> MigrationOptions {
        MigrationOptions {
            max_records: NonZeroU32::new(max_records).unwrap(),
            ..MigrationOptions::default()
        }
    }

    #[test]
    fn checkpoint_roundtrips_with_binary_cursor() {
        let checkpoint = MigrationCheckpoint {
            table: StoreTable::OpLog,
            from_version: 0,
            to_version: 1,
            last_processed_key: Some(vec![0, 1, 2, 255]),
        };

        assert_eq!(
            MigrationCheckpoint::decode(StoreTable::OpLog, &checkpoint.encode().unwrap()).unwrap(),
            checkpoint
        );
    }

    #[test]
    fn registry_applies_sequential_steps_and_intermediate_schema_headers() {
        fn append_one(
            _table: StoreTable,
            key: &[u8],
            value: &[u8],
        ) -> Result<RecordMigration, MigrationError> {
            let mut migrated = value.to_vec();
            migrated.push(b'1');
            Ok(RecordMigration::replace_value(key, migrated))
        }

        fn append_two(
            _table: StoreTable,
            key: &[u8],
            value: &[u8],
        ) -> Result<RecordMigration, MigrationError> {
            let mut migrated = value.to_vec();
            migrated.push(b'2');
            Ok(RecordMigration::replace_value(key, migrated))
        }

        let steps = [
            MigrationStep {
                from_version: 0,
                to_version: 1,
                migrate_record: append_one,
            },
            MigrationStep {
                from_version: 1,
                to_version: 2,
                migrate_record: append_two,
            },
        ];
        let table = StoreTable::LwwRegisterMaterialized;
        validate_migration_path(table, 2, &steps).unwrap();
        let path = tempfile::tempdir().unwrap().keep();
        let store = NxStore::open(&path).unwrap();
        let key = b"__nx/crdt/lww-register/registry-test";
        store.set(key, b"value").unwrap();
        let lease = store.acquire_write_lease().unwrap();

        assert_eq!(
            migrate_table_batch(&lease, table, 2, &steps, options(1)).unwrap(),
            Some(MigrationProgress::BatchValidated {
                table: "lww-register-materialized",
                records: 1,
            })
        );
        assert_eq!(store.get(key).unwrap().unwrap(), b"value1");
        assert!(store.get(&table.schema_key()).unwrap().is_none());
        drop(lease);
        drop(store);

        let store = NxStore::open(&path).unwrap();
        let lease = store.acquire_write_lease().unwrap();
        assert_eq!(
            migrate_table_batch(&lease, table, 2, &steps, options(1)).unwrap(),
            Some(MigrationProgress::TableCompleted {
                table: "lww-register-materialized",
                from_version: 0,
                to_version: 1,
            })
        );
        let v1_header = store.get(&table.schema_key()).unwrap().unwrap();
        assert_eq!(SchemaHeader::decode(table, &v1_header).unwrap().version, 1);

        assert!(matches!(
            migrate_table_batch(&lease, table, 2, &steps, options(1)).unwrap(),
            Some(MigrationProgress::BatchValidated { records: 1, .. })
        ));
        assert_eq!(store.get(key).unwrap().unwrap(), b"value12");

        assert_eq!(
            migrate_table_batch(&lease, table, 2, &steps, options(1)).unwrap(),
            Some(MigrationProgress::TableCompleted {
                table: "lww-register-materialized",
                from_version: 1,
                to_version: 2,
            })
        );
        let v2_header = store.get(&table.schema_key()).unwrap().unwrap();
        assert_eq!(SchemaHeader::decode(table, &v2_header).unwrap().version, 2);
        assert_eq!(
            migrate_table_batch(&lease, table, 2, &steps, options(1)).unwrap(),
            None
        );
    }

    #[test]
    fn registry_rejects_gaps_and_non_sequential_steps() {
        let gap = [
            LEGACY_TO_V1,
            MigrationStep {
                from_version: 2,
                to_version: 3,
                migrate_record: migrate_legacy_record,
            },
        ];
        assert!(matches!(
            validate_migration_path(StoreTable::SeenOps, 3, &gap),
            Err(MigrationError::InvalidRegistry {
                reason: "registry contains a gap or unordered step",
                ..
            })
        ));

        let jump = [MigrationStep {
            from_version: 0,
            to_version: 2,
            migrate_record: migrate_legacy_record,
        }];
        assert!(matches!(
            validate_migration_path(StoreTable::SeenOps, 2, &jump),
            Err(MigrationError::InvalidRegistry {
                reason: "steps must advance exactly one schema version",
                ..
            })
        ));
    }

    #[test]
    fn table_registries_can_advance_independently() {
        let op_log_v2_steps = [
            LEGACY_TO_V1,
            MigrationStep {
                from_version: 1,
                to_version: 2,
                migrate_record: migrate_legacy_record,
            },
        ];

        validate_migration_path(StoreTable::OpLog, 2, &op_log_v2_steps).unwrap();
        validate_migration_path(StoreTable::SeenOps, 1, migration_steps(StoreTable::SeenOps))
            .unwrap();
    }

    #[test]
    fn structured_migrations_support_replace_delete_move_and_additional_sets() {
        fn mutate(
            _table: StoreTable,
            key: &[u8],
            value: &[u8],
        ) -> Result<RecordMigration, MigrationError> {
            if key.ends_with(b"/replace") {
                return Ok(RecordMigration::replace_value(key, b"replaced".to_vec()));
            }
            if key.ends_with(b"/delete") {
                return Ok(RecordMigration::delete(key));
            }
            if key.ends_with(b"/move") {
                return Ok(RecordMigration::move_to(
                    key,
                    b"__nx/archive/moved".to_vec(),
                    value.to_vec(),
                ));
            }
            Ok(RecordMigration::unchanged()
                .with_set(b"__nx/archive/additional".to_vec(), b"created".to_vec()))
        }

        let steps = [MigrationStep {
            from_version: 0,
            to_version: 1,
            migrate_record: mutate,
        }];
        let table = StoreTable::LwwRegisterMaterialized;
        let store = temp_store();
        let prefix = table.data_prefix();
        for suffix in ["replace", "delete", "move", "set"] {
            store
                .set(format!("{prefix}{suffix}").as_bytes(), suffix.as_bytes())
                .unwrap();
        }
        let lease = store.acquire_write_lease().unwrap();

        assert_eq!(
            migrate_table_batch(&lease, table, 1, &steps, options(10)).unwrap(),
            Some(MigrationProgress::BatchValidated {
                table: "lww-register-materialized",
                records: 4,
            })
        );
        assert_eq!(
            store
                .get(format!("{prefix}replace").as_bytes())
                .unwrap()
                .unwrap(),
            b"replaced"
        );
        assert!(
            store
                .get(format!("{prefix}delete").as_bytes())
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .get(format!("{prefix}move").as_bytes())
                .unwrap()
                .is_none()
        );
        assert_eq!(store.get(b"__nx/archive/moved").unwrap().unwrap(), b"move");
        assert_eq!(
            store.get(b"__nx/archive/additional").unwrap().unwrap(),
            b"created"
        );
    }

    #[test]
    fn transformed_batch_limit_failure_preserves_payload_and_checkpoint() {
        fn expand_value(
            _table: StoreTable,
            key: &[u8],
            _value: &[u8],
        ) -> Result<RecordMigration, MigrationError> {
            Ok(RecordMigration::replace_value(key, vec![b'x'; 128]))
        }

        let steps = [MigrationStep {
            from_version: 0,
            to_version: 1,
            migrate_record: expand_value,
        }];
        let table = StoreTable::LwwRegisterMaterialized;
        let store = temp_store();
        let key = b"__nx/crdt/lww-register/output-limit";
        store.set(key, b"x").unwrap();
        let lease = store.acquire_write_lease().unwrap();
        let options = MigrationOptions {
            max_records: NonZeroU32::new(1).unwrap(),
            max_bytes: NonZeroUsize::new(64).unwrap(),
        };

        assert!(matches!(
            migrate_table_batch(&lease, table, 1, &steps, options),
            Err(MigrationError::BatchMemoryLimitExceeded { .. })
        ));
        assert_eq!(store.get(key).unwrap().unwrap(), b"x");
        assert!(store.get(&checkpoint_key(table)).unwrap().is_none());
        assert!(store.get(&table.schema_key()).unwrap().is_none());
    }

    #[test]
    fn structured_migrations_cannot_modify_engine_metadata() {
        fn overwrite_schema(
            table: StoreTable,
            _key: &[u8],
            _value: &[u8],
        ) -> Result<RecordMigration, MigrationError> {
            Ok(RecordMigration::unchanged().with_set(table.schema_key(), b"bad".to_vec()))
        }

        let steps = [MigrationStep {
            from_version: 0,
            to_version: 1,
            migrate_record: overwrite_schema,
        }];
        let table = StoreTable::LwwRegisterMaterialized;
        let store = temp_store();
        store
            .set(b"__nx/crdt/lww-register/metadata-test", b"value")
            .unwrap();
        let lease = store.acquire_write_lease().unwrap();

        assert!(matches!(
            migrate_table_batch(&lease, table, 1, &steps, options(1)),
            Err(MigrationError::InvalidMutation {
                reason: "schema and migration metadata are managed by the engine",
                ..
            })
        ));
        assert!(store.get(&table.schema_key()).unwrap().is_none());
        assert!(store.get(&checkpoint_key(table)).unwrap().is_none());
    }

    #[test]
    fn structured_migrations_cannot_write_sibling_keys_in_source_namespace() {
        fn write_sibling(
            table: StoreTable,
            _key: &[u8],
            _value: &[u8],
        ) -> Result<RecordMigration, MigrationError> {
            Ok(RecordMigration::unchanged().with_set(
                format!("{}aaa", table.data_prefix()).into_bytes(),
                b"bad".to_vec(),
            ))
        }

        let steps = [MigrationStep {
            from_version: 0,
            to_version: 1,
            migrate_record: write_sibling,
        }];
        let table = StoreTable::LwwRegisterMaterialized;
        let store = temp_store();
        store
            .set(
                format!("{}middle", table.data_prefix()).as_bytes(),
                b"value",
            )
            .unwrap();
        let lease = store.acquire_write_lease().unwrap();

        assert!(matches!(
            migrate_table_batch(&lease, table, 1, &steps, options(1)),
            Err(MigrationError::InvalidMutation {
                reason: "record migrations can only mutate their source key inside the source namespace",
                ..
            })
        ));
        assert!(
            store
                .get(format!("{}aaa", table.data_prefix()).as_bytes())
                .unwrap()
                .is_none()
        );
        assert!(store.get(&checkpoint_key(table)).unwrap().is_none());
        assert!(store.get(&table.schema_key()).unwrap().is_none());
    }

    #[test]
    fn structured_migrations_cannot_delete_sibling_keys_in_source_namespace() {
        fn delete_sibling(
            table: StoreTable,
            _key: &[u8],
            _value: &[u8],
        ) -> Result<RecordMigration, MigrationError> {
            Ok(RecordMigration::delete(
                format!("{}zzz", table.data_prefix()).as_bytes(),
            ))
        }

        let steps = [MigrationStep {
            from_version: 0,
            to_version: 1,
            migrate_record: delete_sibling,
        }];
        let table = StoreTable::LwwRegisterMaterialized;
        let store = temp_store();
        store
            .set(
                format!("{}middle", table.data_prefix()).as_bytes(),
                b"value",
            )
            .unwrap();
        store
            .set(format!("{}zzz", table.data_prefix()).as_bytes(), b"future")
            .unwrap();
        let lease = store.acquire_write_lease().unwrap();

        assert!(matches!(
            migrate_table_batch(&lease, table, 1, &steps, options(1)),
            Err(MigrationError::InvalidMutation {
                reason: "record migrations can only mutate their source key inside the source namespace",
                ..
            })
        ));
        assert_eq!(
            store
                .get(format!("{}zzz", table.data_prefix()).as_bytes())
                .unwrap()
                .as_deref(),
            Some(b"future".as_slice())
        );
        assert!(store.get(&checkpoint_key(table)).unwrap().is_none());
        assert!(store.get(&table.schema_key()).unwrap().is_none());
    }

    #[test]
    fn migrates_empty_store_to_v1() {
        let store = temp_store();
        migrate_sync_schema(&store, options(2)).unwrap();

        for table in StoreTable::ALL {
            let header = store.get(&table.schema_key()).unwrap().unwrap();
            assert_eq!(
                SchemaHeader::decode(table, &header).unwrap(),
                SchemaHeader::current(table)
            );
            assert!(store.get(&checkpoint_key(table)).unwrap().is_none());
        }
    }

    #[test]
    fn validates_legacy_records_in_bounded_batches_and_resumes() {
        let path = tempfile::tempdir().unwrap().keep();
        let store = NxStore::open(&path).unwrap();
        let mut original_records = Vec::new();
        for index in 0..5 {
            let key = materialized_gcounter_key(&format!("counter:{index}"));
            let value = (index as u64).to_le_bytes().to_vec();
            store.set(&key, &value).unwrap();
            original_records.push((key, value));
        }

        assert_eq!(
            SyncSchemaMigration::new(&store, options(2))
                .unwrap()
                .step()
                .unwrap(),
            MigrationProgress::BatchValidated {
                table: "gcounter-materialized",
                records: 2,
            }
        );
        let checkpoint = store
            .get(&checkpoint_key(StoreTable::GCounterMaterialized))
            .unwrap()
            .unwrap();
        let checkpoint =
            MigrationCheckpoint::decode(StoreTable::GCounterMaterialized, &checkpoint).unwrap();
        assert!(checkpoint.last_processed_key.is_some());

        drop(store);
        let store = NxStore::open(&path).unwrap();
        migrate_sync_schema(&store, options(2)).unwrap();
        assert!(
            store
                .get(&checkpoint_key(StoreTable::GCounterMaterialized))
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .get(&StoreTable::GCounterMaterialized.schema_key())
                .unwrap()
                .is_some()
        );
        for (key, value) in original_records {
            assert_eq!(store.get(&key).unwrap(), Some(value));
        }
    }

    #[test]
    fn migration_session_blocks_writes_between_steps() {
        let store = temp_store();
        for index in 0..3 {
            store
                .set(
                    &materialized_gcounter_key(&format!("counter:{index}")),
                    &(index as u64).to_le_bytes(),
                )
                .unwrap();
        }

        let mut migration = SyncSchemaMigration::new(&store, options(1)).unwrap();
        assert!(matches!(
            migration.step().unwrap(),
            MigrationProgress::BatchValidated { records: 1, .. }
        ));

        let writer_store = Arc::clone(&store);
        let (started_sender, started_receiver) = mpsc::channel();
        let (completed_sender, completed_receiver) = mpsc::channel();
        let writer = std::thread::spawn(move || {
            started_sender.send(()).unwrap();
            writer_store
                .set(
                    &materialized_gcounter_key("counter:new"),
                    &9u64.to_le_bytes(),
                )
                .unwrap();
            completed_sender.send(()).unwrap();
        });

        started_receiver
            .recv_timeout(Duration::from_secs(1))
            .unwrap();
        assert!(
            completed_receiver
                .recv_timeout(Duration::from_millis(50))
                .is_err()
        );
        assert!(matches!(
            migration.step().unwrap(),
            MigrationProgress::BatchValidated { records: 1, .. }
        ));
        assert!(
            completed_receiver
                .recv_timeout(Duration::from_millis(50))
                .is_err()
        );

        drop(migration);
        completed_receiver
            .recv_timeout(Duration::from_secs(1))
            .unwrap();
        writer.join().unwrap();
    }

    #[test]
    fn migrates_fixture_generated_by_v0_1_0_without_changing_payloads() {
        let records = parse_fixture(include_str!("fixtures/v0_1_0/records.hex"));
        assert_eq!(records.len(), StoreTable::ALL.len());
        assert!(
            records
                .iter()
                .all(|(key, _)| !key.starts_with(b"__nx/schema/"))
        );

        let store = temp_store();
        for (key, value) in &records {
            store.set(key, value).unwrap();
        }

        migrate_sync_schema(&store, options(2)).unwrap();

        for (key, expected_value) in &records {
            assert_eq!(
                store.get(key).unwrap().as_deref(),
                Some(expected_value.as_slice())
            );
        }
        for table in StoreTable::ALL {
            let header = store.get(&table.schema_key()).unwrap().unwrap();
            assert_eq!(
                SchemaHeader::decode(table, &header).unwrap(),
                SchemaHeader::current(table)
            );
        }

        let gcounter = parse_durable_gcounter_state(
            &store
                .get(b"__nx/crdt/state/gcounter/visits")
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(gcounter.value(), 12);

        let pncounter = parse_durable_pncounter_state(
            &store
                .get(b"__nx/crdt/state/pncounter/stock")
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(pncounter.value(), 6);

        let register = parse_durable_lww_register_state(
            &store
                .get(b"__nx/crdt/state/lww-register/status")
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(register.value(), b"online");

        let map = parse_durable_lww_map_state(
            &store
                .get(b"__nx/crdt/state/lww-map/settings")
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(map.get("region"), Some(b"eu".as_slice()));
        assert!(!map.contains("obsolete"));

        let set =
            parse_durable_orset_state(&store.get(b"__nx/crdt/state/orset/tags").unwrap().unwrap())
                .unwrap();
        assert_eq!(set.elements(), vec!["blue"]);

        let rga =
            parse_durable_rga_state(&store.get(b"__nx/crdt/state/rga/document").unwrap().unwrap())
                .unwrap();
        assert_eq!(rga.values(), vec![b"hello".to_vec()]);

        assert_eq!(
            parse_seen_op_sequence(
                &store
                    .get(b"__nx/crdt/seen-op/fixture-op-1")
                    .unwrap()
                    .unwrap()
            )
            .unwrap(),
            7
        );
        let (sequence, op) = parse_durable_op_log_value(
            &store
                .get(b"__nx/crdt/op-log/fixture-op-1")
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(sequence, 9);
        assert_eq!(op.id.as_str(), "fixture-op-1");
    }

    fn parse_fixture(fixture: &str) -> Vec<(Vec<u8>, Vec<u8>)> {
        fixture
            .lines()
            .filter(|line| !line.is_empty() && !line.starts_with('#'))
            .map(|line| {
                let (key, value) = line
                    .split_once('\t')
                    .expect("fixture record must contain a tab separator");
                (decode_hex(key), decode_hex(value))
            })
            .collect()
    }

    fn decode_hex(hex: &str) -> Vec<u8> {
        assert!(
            hex.len().is_multiple_of(2),
            "hex input must have even length"
        );
        hex.as_bytes()
            .chunks_exact(2)
            .map(|pair| {
                let high = decode_nibble(pair[0]);
                let low = decode_nibble(pair[1]);
                (high << 4) | low
            })
            .collect()
    }

    fn decode_nibble(byte: u8) -> u8 {
        match byte {
            b'0'..=b'9' => byte - b'0',
            b'a'..=b'f' => byte - b'a' + 10,
            _ => panic!("fixture contains non-lowercase-hex byte"),
        }
    }

    #[test]
    fn migrates_explicit_v0_header_to_v1() {
        let store = temp_store();
        let table = StoreTable::SeenOps;
        let v0_header = SchemaHeader { version: 0, table }.encode();
        store.set(&table.schema_key(), &v0_header).unwrap();
        store
            .set(b"__nx/crdt/seen-op/op-a", &7u64.to_be_bytes())
            .unwrap();

        migrate_sync_schema(&store, options(1)).unwrap();

        let header = store.get(&table.schema_key()).unwrap().unwrap();
        assert_eq!(
            SchemaHeader::decode(table, &header).unwrap(),
            SchemaHeader::current(table)
        );
    }

    #[test]
    fn invalid_record_does_not_advance_checkpoint_or_write_schema() {
        let store = temp_store();
        store
            .set(&materialized_gcounter_key("counter:bad"), b"bad")
            .unwrap();

        assert!(matches!(
            SyncSchemaMigration::new(&store, MigrationOptions::default())
                .unwrap()
                .step(),
            Err(MigrationError::InvalidRecord {
                table: "gcounter-materialized",
                ..
            })
        ));
        assert!(
            store
                .get(&checkpoint_key(StoreTable::GCounterMaterialized))
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .get(&StoreTable::GCounterMaterialized.schema_key())
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn rejects_record_larger_than_batch_byte_limit() {
        let store = temp_store();
        store
            .set(b"__nx/crdt/lww-register/status:user-1", &[7; 128])
            .unwrap();
        let options = MigrationOptions {
            max_records: NonZeroU32::new(10).unwrap(),
            max_bytes: NonZeroUsize::new(64).unwrap(),
        };

        assert!(matches!(
            migrate_sync_schema(&store, options),
            Err(MigrationError::Store(
                nx_store::StoreError::ScanBatchByteLimitExceeded { .. }
            ))
        ));
    }

    #[test]
    fn validates_durable_state_and_op_log() {
        let store = temp_store();
        let node_id = NodeId::new("node-a");
        let mut counter = GCounter::new();
        counter.increment(&node_id, 3);
        store
            .set(
                &durable_gcounter_state_key("counter:visits"),
                counter.to_json().unwrap().as_bytes(),
            )
            .unwrap();

        let op = Op::gcounter_increment(node_id, "counter:visits", 1);
        store
            .set(
                &op_log_store_key(op.id.as_str()),
                &encode_durable_op_log_value(1, &op).unwrap(),
            )
            .unwrap();

        migrate_sync_schema(&store, options(1)).unwrap();
    }

    #[test]
    fn rejects_op_log_key_value_mismatch() {
        let store = temp_store();
        let op = Op::gcounter_increment(NodeId::new("node-a"), "counter:visits", 1);
        store
            .set(
                &op_log_store_key("different-id"),
                &encode_durable_op_log_value(1, &op).unwrap(),
            )
            .unwrap();

        assert!(matches!(
            migrate_sync_schema(&store, MigrationOptions::default()),
            Err(MigrationError::InvalidRecord {
                table: "op-log",
                ..
            })
        ));
    }

    #[test]
    fn rejects_corrupted_checkpoint() {
        let store = temp_store();
        let table = StoreTable::SeenOps;
        store.set(&checkpoint_key(table), b"bad").unwrap();

        assert!(matches!(
            SyncSchemaMigration::new(&store, MigrationOptions::default())
                .unwrap()
                .step(),
            Err(MigrationError::InvalidCheckpoint {
                table: "seen-ops",
                ..
            })
        ));
    }

    #[test]
    fn rejects_checkpoint_cursor_outside_table_prefix() {
        let store = temp_store();
        let table = StoreTable::SeenOps;
        let checkpoint = MigrationCheckpoint {
            table,
            from_version: 0,
            to_version: 1,
            last_processed_key: Some(b"__nx/crdt/op-log/wrong-table".to_vec()),
        };
        store
            .set(&checkpoint_key(table), &checkpoint.encode().unwrap())
            .unwrap();

        assert!(matches!(
            SyncSchemaMigration::new(&store, MigrationOptions::default())
                .unwrap()
                .step(),
            Err(MigrationError::InvalidCheckpoint {
                table: "seen-ops",
                reason: "cursor is outside the table prefix"
            })
        ));
    }

    #[test]
    fn rejects_stale_checkpoint_for_completed_table() {
        let store = temp_store();
        let table = StoreTable::GCounterMaterialized;
        let header = SchemaHeader::current(table).encode();
        let checkpoint = MigrationCheckpoint::new(table, 0, 1).encode().unwrap();
        store.set(&table.schema_key(), &header).unwrap();
        store.set(&checkpoint_key(table), &checkpoint).unwrap();

        assert!(matches!(
            SyncSchemaMigration::new(&store, MigrationOptions::default())
                .unwrap()
                .step(),
            Err(MigrationError::StaleCheckpoint {
                table: "gcounter-materialized"
            })
        ));
    }
}
