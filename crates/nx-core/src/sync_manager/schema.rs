use std::fmt;

use nx_store::Store as NxStore;

use super::types::{
    GCOUNTER_STATE_STORE_PREFIX, GCOUNTER_STORE_PREFIX, LWW_MAP_STATE_STORE_PREFIX,
    LWW_MAP_STORE_PREFIX, LWW_REGISTER_STATE_STORE_PREFIX, LWW_REGISTER_STORE_PREFIX,
    OP_LOG_STORE_PREFIX, ORSET_STATE_STORE_PREFIX, ORSET_STORE_PREFIX,
    PNCOUNTER_STATE_STORE_PREFIX, PNCOUNTER_STORE_PREFIX, RGA_STATE_STORE_PREFIX, RGA_STORE_PREFIX,
    SEEN_OP_STORE_PREFIX,
};

const SCHEMA_MAGIC: [u8; 4] = *b"NXDB";
const SCHEMA_HEADER_LEN: usize = 8;
const SCHEMA_KEY_PREFIX: &str = "__nx/schema/";
const INITIAL_SCHEMA_VERSION: u16 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub(super) enum StoreTable {
    GCounterMaterialized = 1,
    GCounterState = 2,
    PNCounterMaterialized = 3,
    PNCounterState = 4,
    LwwRegisterMaterialized = 5,
    LwwRegisterState = 6,
    LwwMapMaterialized = 7,
    LwwMapState = 8,
    ORSetMaterialized = 9,
    ORSetState = 10,
    RgaMaterialized = 11,
    RgaState = 12,
    SeenOps = 13,
    OpLog = 14,
}

impl StoreTable {
    const ALL: [Self; 14] = [
        Self::GCounterMaterialized,
        Self::GCounterState,
        Self::PNCounterMaterialized,
        Self::PNCounterState,
        Self::LwwRegisterMaterialized,
        Self::LwwRegisterState,
        Self::LwwMapMaterialized,
        Self::LwwMapState,
        Self::ORSetMaterialized,
        Self::ORSetState,
        Self::RgaMaterialized,
        Self::RgaState,
        Self::SeenOps,
        Self::OpLog,
    ];

    fn name(self) -> &'static str {
        match self {
            Self::GCounterMaterialized => "gcounter-materialized",
            Self::GCounterState => "gcounter-state",
            Self::PNCounterMaterialized => "pncounter-materialized",
            Self::PNCounterState => "pncounter-state",
            Self::LwwRegisterMaterialized => "lww-register-materialized",
            Self::LwwRegisterState => "lww-register-state",
            Self::LwwMapMaterialized => "lww-map-materialized",
            Self::LwwMapState => "lww-map-state",
            Self::ORSetMaterialized => "orset-materialized",
            Self::ORSetState => "orset-state",
            Self::RgaMaterialized => "rga-materialized",
            Self::RgaState => "rga-state",
            Self::SeenOps => "seen-ops",
            Self::OpLog => "op-log",
        }
    }

    fn data_prefix(self) -> &'static str {
        match self {
            Self::GCounterMaterialized => GCOUNTER_STORE_PREFIX,
            Self::GCounterState => GCOUNTER_STATE_STORE_PREFIX,
            Self::PNCounterMaterialized => PNCOUNTER_STORE_PREFIX,
            Self::PNCounterState => PNCOUNTER_STATE_STORE_PREFIX,
            Self::LwwRegisterMaterialized => LWW_REGISTER_STORE_PREFIX,
            Self::LwwRegisterState => LWW_REGISTER_STATE_STORE_PREFIX,
            Self::LwwMapMaterialized => LWW_MAP_STORE_PREFIX,
            Self::LwwMapState => LWW_MAP_STATE_STORE_PREFIX,
            Self::ORSetMaterialized => ORSET_STORE_PREFIX,
            Self::ORSetState => ORSET_STATE_STORE_PREFIX,
            Self::RgaMaterialized => RGA_STORE_PREFIX,
            Self::RgaState => RGA_STATE_STORE_PREFIX,
            Self::SeenOps => SEEN_OP_STORE_PREFIX,
            Self::OpLog => OP_LOG_STORE_PREFIX,
        }
    }

    fn current_version(self) -> u16 {
        INITIAL_SCHEMA_VERSION
    }

    fn schema_key(self) -> Vec<u8> {
        format!("{SCHEMA_KEY_PREFIX}{}", self.name()).into_bytes()
    }
}

impl fmt::Display for StoreTable {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.name())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SchemaHeader {
    version: u16,
    table: StoreTable,
}

impl SchemaHeader {
    fn current(table: StoreTable) -> Self {
        Self {
            version: table.current_version(),
            table,
        }
    }

    fn encode(self) -> [u8; SCHEMA_HEADER_LEN] {
        let mut bytes = [0; SCHEMA_HEADER_LEN];
        bytes[..4].copy_from_slice(&SCHEMA_MAGIC);
        bytes[4..6].copy_from_slice(&self.version.to_be_bytes());
        bytes[6..8].copy_from_slice(&(self.table as u16).to_be_bytes());
        bytes
    }

    fn decode(table: StoreTable, bytes: &[u8]) -> Result<Self, SchemaError> {
        if bytes.len() != SCHEMA_HEADER_LEN {
            return Err(SchemaError::InvalidHeaderLength {
                table,
                actual: bytes.len(),
            });
        }
        if bytes[..4] != SCHEMA_MAGIC {
            return Err(SchemaError::InvalidMagic { table });
        }

        let version = u16::from_be_bytes([bytes[4], bytes[5]]);
        let actual_table = u16::from_be_bytes([bytes[6], bytes[7]]);
        if actual_table != table as u16 {
            return Err(SchemaError::TableMismatch {
                expected: table,
                actual: actual_table,
            });
        }

        Ok(Self { version, table })
    }
}

#[derive(Debug)]
pub(super) enum SchemaError {
    Store(nx_store::StoreError),
    InvalidHeaderLength {
        table: StoreTable,
        actual: usize,
    },
    InvalidMagic {
        table: StoreTable,
    },
    TableMismatch {
        expected: StoreTable,
        actual: u16,
    },
    LegacyTable {
        table: StoreTable,
    },
    OlderVersion {
        table: StoreTable,
        found: u16,
        current: u16,
    },
    FutureVersion {
        table: StoreTable,
        found: u16,
        current: u16,
    },
}

impl fmt::Display for SchemaError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Store(error) => write!(formatter, "schema store error: {error}"),
            Self::InvalidHeaderLength { table, actual } => write!(
                formatter,
                "invalid schema header length for {table}: expected {SCHEMA_HEADER_LEN}, got {actual}"
            ),
            Self::InvalidMagic { table } => {
                write!(formatter, "invalid schema magic for {table}")
            }
            Self::TableMismatch { expected, actual } => write!(
                formatter,
                "schema table mismatch: expected {expected}, got table id {actual}"
            ),
            Self::LegacyTable { table } => write!(
                formatter,
                "legacy unversioned data found in {table}; an explicit migration is required"
            ),
            Self::OlderVersion {
                table,
                found,
                current,
            } => write!(
                formatter,
                "schema migration required for {table}: found version {found}, current version is {current}"
            ),
            Self::FutureVersion {
                table,
                found,
                current,
            } => write!(
                formatter,
                "unsupported future schema for {table}: found version {found}, current version is {current}"
            ),
        }
    }
}

impl std::error::Error for SchemaError {}

impl From<nx_store::StoreError> for SchemaError {
    fn from(error: nx_store::StoreError) -> Self {
        Self::Store(error)
    }
}

pub(super) fn ensure_sync_schema(store: &NxStore) -> Result<(), SchemaError> {
    let mut missing_tables = Vec::new();
    for table in StoreTable::ALL {
        let schema_key = table.schema_key();
        match store.get(&schema_key)? {
            Some(bytes) => validate_header(table, &bytes)?,
            None => {
                if !store
                    .scan_prefix(table.data_prefix().as_bytes())?
                    .is_empty()
                {
                    return Err(SchemaError::LegacyTable { table });
                }
                missing_tables.push(table);
            }
        }
    }

    if missing_tables.is_empty() {
        return Ok(());
    }

    let keys = missing_tables
        .iter()
        .map(|table| table.schema_key())
        .collect::<Vec<_>>();
    let values = missing_tables
        .iter()
        .map(|table| SchemaHeader::current(*table).encode())
        .collect::<Vec<_>>();
    let sets = keys
        .iter()
        .zip(values.iter())
        .map(|(key, value)| (key.as_slice(), value.as_slice()))
        .collect::<Vec<_>>();
    store.apply_batch(&sets, &[])?;
    Ok(())
}

fn validate_header(table: StoreTable, bytes: &[u8]) -> Result<(), SchemaError> {
    let header = SchemaHeader::decode(table, bytes)?;
    let current = table.current_version();
    if header.version < current {
        return Err(SchemaError::OlderVersion {
            table,
            found: header.version,
            current,
        });
    }
    if header.version > current {
        return Err(SchemaError::FutureVersion {
            table,
            found: header.version,
            current,
        });
    }
    Ok(())
}

// End of main code. Test below:

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    fn temp_store() -> Arc<NxStore> {
        Arc::new(NxStore::open(tempfile::tempdir().unwrap().keep()).unwrap())
    }

    #[test]
    fn schema_header_roundtrips() {
        let header = SchemaHeader::current(StoreTable::OpLog);
        assert_eq!(
            SchemaHeader::decode(StoreTable::OpLog, &header.encode()).unwrap(),
            header
        );
    }

    #[test]
    fn initializes_all_empty_tables() {
        let store = temp_store();
        ensure_sync_schema(&store).unwrap();

        for table in StoreTable::ALL {
            let bytes = store.get(&table.schema_key()).unwrap().unwrap();
            assert_eq!(
                SchemaHeader::decode(table, &bytes).unwrap(),
                SchemaHeader::current(table)
            );
        }
    }

    #[test]
    fn initialization_is_idempotent() {
        let store = temp_store();
        ensure_sync_schema(&store).unwrap();
        ensure_sync_schema(&store).unwrap();
    }

    #[test]
    fn rejects_invalid_magic() {
        let store = temp_store();
        let table = StoreTable::SeenOps;
        store.set(&table.schema_key(), b"BAD!\0\x01\0\r").unwrap();

        assert!(matches!(
            ensure_sync_schema(&store),
            Err(SchemaError::InvalidMagic {
                table: StoreTable::SeenOps
            })
        ));
    }

    #[test]
    fn rejects_truncated_header() {
        let store = temp_store();
        let table = StoreTable::OpLog;
        store.set(&table.schema_key(), b"NXDB").unwrap();

        assert!(matches!(
            ensure_sync_schema(&store),
            Err(SchemaError::InvalidHeaderLength {
                table: StoreTable::OpLog,
                actual: 4
            })
        ));
    }

    #[test]
    fn rejects_table_id_mismatch() {
        let store = temp_store();
        let table = StoreTable::OpLog;
        let wrong_header = SchemaHeader::current(StoreTable::SeenOps).encode();
        store.set(&table.schema_key(), &wrong_header).unwrap();

        assert!(matches!(
            ensure_sync_schema(&store),
            Err(SchemaError::TableMismatch {
                expected: StoreTable::OpLog,
                actual
            }) if actual == StoreTable::SeenOps as u16
        ));
    }

    #[test]
    fn rejects_legacy_unversioned_table() {
        let store = temp_store();
        store
            .set(b"__nx/crdt/op-log/legacy-op", b"legacy-value")
            .unwrap();

        assert!(matches!(
            ensure_sync_schema(&store),
            Err(SchemaError::LegacyTable {
                table: StoreTable::OpLog
            })
        ));
        assert!(
            StoreTable::ALL
                .iter()
                .all(|table| store.get(&table.schema_key()).unwrap().is_none())
        );
    }

    #[test]
    fn rejects_older_schema_version() {
        let store = temp_store();
        let table = StoreTable::RgaState;
        let mut header = SchemaHeader::current(table);
        header.version = 0;
        store.set(&table.schema_key(), &header.encode()).unwrap();

        assert!(matches!(
            ensure_sync_schema(&store),
            Err(SchemaError::OlderVersion {
                table: StoreTable::RgaState,
                found: 0,
                current: 1
            })
        ));
    }

    #[test]
    fn rejects_future_schema_version() {
        let store = temp_store();
        let table = StoreTable::GCounterState;
        let mut header = SchemaHeader::current(table);
        header.version += 1;
        store.set(&table.schema_key(), &header.encode()).unwrap();

        assert!(matches!(
            ensure_sync_schema(&store),
            Err(SchemaError::FutureVersion {
                table: StoreTable::GCounterState,
                found: 2,
                current: 1
            })
        ));
    }
}
