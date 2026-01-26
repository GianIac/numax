use thiserror::Error;

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("sled error: {0}")]
    Sled(#[from] sled::Error),

    #[error("invalid path")]
    InvalidPath,
}
