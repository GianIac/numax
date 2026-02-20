use thiserror::Error;

pub type SyncResult<T> = Result<T, SyncError>;

#[derive(Debug, Error)]
pub enum SyncError {
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("unknown node: {0}")]
    UnknownNode(String),

    #[error("duplicate operation: {0}")]
    DuplicateOp(String),

    #[error("invalid operation: {0}")]
    InvalidOp(String),
}