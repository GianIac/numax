mod apply;
mod baseline;
mod candidates;
pub(crate) mod delivery;
pub(crate) use candidates::canonicalize_endpoint;
#[cfg(test)]
mod fixtures;
mod manager;
mod manual;
mod migration;
mod neighbors;
mod pacing;
mod peer;
mod replication;
mod schema;
mod sequence;
mod storage;
pub(crate) fn storage_progress(
    store: &nx_store::Store,
) -> anyhow::Result<Vec<ReplicationProgress>> {
    storage::replication_progress(store)
}
pub use sequence::{RecoveryFailure, ReplicationProgress, UnavailableHistory};
mod types;

pub use manager::{SyncHandle, SyncManager};
pub use migration::{
    DEFAULT_MIGRATION_BATCH_BYTES, DEFAULT_MIGRATION_BATCH_SIZE, MigrationError, MigrationOptions,
    MigrationProgress, SyncSchemaMigration, migrate_sync_schema, migrate_sync_schema_at_path,
};
pub use peer::PeerHealthState;
