use thiserror::Error;

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("sled error: {0}")]
    Sled(#[from] sled::Error),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("store path exists but is not a directory: {0}")]
    NotADirectory(String),

    #[error("record size {record_bytes} exceeds scan batch byte limit {max_bytes}")]
    ScanBatchByteLimitExceeded {
        record_bytes: usize,
        max_bytes: usize,
    },

    #[error("store write lock is poisoned")]
    WriteLockPoisoned,
}
