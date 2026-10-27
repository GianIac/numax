use thiserror::Error;

use crate::message::WireError;

pub type NetResult<T> = Result<T, NetError>;
pub type BootstrapResult<T> = Result<T, BootstrapError>;

#[derive(Debug, Error)]
pub enum NetError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("binary serialization error: {0}")]
    BinarySerialization(#[from] wincode::WriteError),

    #[error("binary deserialization error: {0}")]
    BinaryDeserialization(#[from] wincode::ReadError),

    #[error("connection failed: {0}")]
    ConnectionFailed(String),

    #[error("peer disconnected: {0}")]
    PeerDisconnected(String),

    #[error("invalid message: {0}")]
    InvalidMessage(String),

    #[error("wire error: {0}")]
    Wire(WireError),

    #[error("message too large: {len} > {limit}")]
    MessageTooLarge { len: usize, limit: usize },

    #[error("timeout")]
    Timeout,

    #[error("channel closed")]
    ChannelClosed,

    #[error("TLS error: {0}")]
    TlsError(String),

    #[error("peer not allowed: {0}")]
    PeerNotAllowed(String),

    #[error("peer connection limit reached: {0}")]
    PeerLimitReached(usize),

    #[error("node ID mismatch: expected {expected}, got {got}")]
    NodeIdMismatch { expected: String, got: String },
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum NodeConfigError {
    #[error("max_peers must not exceed {limit}")]
    MaxPeersTooLarge { limit: usize },

    #[error("event_channel_capacity must be in 1..={limit}")]
    InvalidEventChannelCapacity { limit: usize },

    #[error("socket_timeout must be positive and form a representable deadline")]
    InvalidSocketTimeout,
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum BootstrapError {
    #[error("invalid bootstrap configuration: {0}")]
    InvalidConfig(String),

    #[error("bootstrap query concurrency limit reached: {limit}")]
    ConcurrencyLimitReached { limit: usize },

    #[error("bootstrap request rejected: {reason}")]
    Rejected { reason: String },

    #[error("invalid bootstrap response: {0}")]
    InvalidResponse(String),

    #[error("bootstrap transport error: {0}")]
    Transport(#[source] NetError),

    #[error("invalid node configuration: {0}")]
    NodeConfig(#[from] NodeConfigError),
}

impl From<NetError> for BootstrapError {
    fn from(error: NetError) -> Self {
        Self::Transport(error)
    }
}
