//! Error for nx-net.

use thiserror::Error;

pub type NetResult<T> = Result<T, NetError>;

#[derive(Debug, Error)]
pub enum NetError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("connection failed: {0}")]
    ConnectionFailed(String),

    #[error("peer disconnected: {0}")]
    PeerDisconnected(String),

    #[error("invalid message: {0}")]
    InvalidMessage(String),

    #[error("timeout")]
    Timeout,

    #[error("channel closed")]
    ChannelClosed,

    #[error("TLS error: {0}")]
    TlsError(String),

    #[error("peer not allowed: {0}")]
    PeerNotAllowed(String),

    #[error("node ID mismatch: expected {expected}, got {got}")]
    NodeIdMismatch { expected: String, got: String },
}
