use thiserror::Error;

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("sled error: {0}")]
    Sled(#[from] sled::Error),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("store path exists but is not a directory: {0}")]
    NotADirectory(String),
}
