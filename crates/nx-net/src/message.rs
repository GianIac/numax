use std::time::Duration;

use crate::{NetError, NetResult};
use nx_sync::{NodeId, Op};
use serde::{Deserialize, Serialize};

/// Protocol version.
pub const PROTOCOL_VERSION: u32 = 4;

const FORMAT_JSON: u8 = 0x01;
const FORMAT_BINCODE: u8 = 0x02;

/// Wire payload serialization format.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SerializationFormat {
    Json,
    Bincode,
}

impl SerializationFormat {
    fn to_wire_byte(self) -> u8 {
        match self {
            Self::Json => FORMAT_JSON,
            Self::Bincode => FORMAT_BINCODE,
        }
    }

    fn from_wire_byte(byte: u8) -> NetResult<Self> {
        match byte {
            FORMAT_JSON => Ok(Self::Json),
            FORMAT_BINCODE => Ok(Self::Bincode),
            other => Err(NetError::InvalidMessage(format!(
                "unknown serialization format byte: {other}"
            ))),
        }
    }
}

pub const DEFAULT_SUPPORTED_FORMATS: &[SerializationFormat] =
    &[SerializationFormat::Bincode, SerializationFormat::Json];

/// Structured error sent over the wire before closing or rejecting a request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WireError {
    ProtocolMismatch { expected: u32, got: u32 },
    OpRejected { reason: String },
    RateLimited { retry_after_ms: Option<u64> },
    NotAuthorized { reason: String },
    Internal { reason: String },
}

/// Reconnect behavior implied by a structured wire error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WireRetryPolicy {
    Fatal,
    Retry,
    RetryAfter(Duration),
    RequestFatal,
}

impl WireError {
    pub fn protocol_mismatch(got: u32) -> Self {
        Self::ProtocolMismatch {
            expected: PROTOCOL_VERSION,
            got,
        }
    }

    pub fn retry_policy(&self) -> WireRetryPolicy {
        match self {
            Self::ProtocolMismatch { .. } | Self::NotAuthorized { .. } => WireRetryPolicy::Fatal,
            Self::RateLimited {
                retry_after_ms: Some(retry_after_ms),
            } => WireRetryPolicy::RetryAfter(Duration::from_millis(*retry_after_ms)),
            Self::RateLimited {
                retry_after_ms: None,
            }
            | Self::Internal { .. } => WireRetryPolicy::Retry,
            Self::OpRejected { .. } => WireRetryPolicy::RequestFatal,
        }
    }
}

impl std::fmt::Display for WireError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ProtocolMismatch { expected, got } => {
                write!(
                    formatter,
                    "protocol version mismatch: expected {expected}, got {got}"
                )
            }
            Self::OpRejected { reason } => write!(formatter, "op rejected: {reason}"),
            Self::RateLimited { retry_after_ms } => match retry_after_ms {
                Some(retry_after_ms) => write!(
                    formatter,
                    "rate limited: retry after {retry_after_ms} milliseconds"
                ),
                None => formatter.write_str("rate limited"),
            },
            Self::NotAuthorized { reason } => write!(formatter, "not authorized: {reason}"),
            Self::Internal { reason } => write!(formatter, "internal wire error: {reason}"),
        }
    }
}

/// Message type.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MessageKind {
    /// Initial handshake.
    Hello {
        node_id: NodeId,
        #[serde(alias = "version")]
        protocol_version: u32,
        supported_formats: Vec<SerializationFormat>,
        preferred_format: SerializationFormat,
    },

    /// Response to Hello.
    HelloAck {
        node_id: NodeId,
        #[serde(alias = "version")]
        protocol_version: u32,
        selected_format: SerializationFormat,
    },

    /// Send CRDT operations.
    PushOps { ops: Vec<Op> },

    /// Acknowledge ops reception.
    PushOpsAck { received_count: u64 },

    /// Request operations from a certain point.
    /// `since_op_id` is the last known op_id (None = I want everything).
    PullSince { since_op_id: Option<String> },

    /// Ping for keepalive.
    Ping,

    /// Response to Ping.
    Pong,

    /// Structured protocol error.
    Error { error: WireError },
}

/// Complete message with metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Message {
    pub kind: MessageKind,
}

impl Message {
    pub fn hello(node_id: NodeId) -> Self {
        Self::hello_with_formats(
            node_id,
            DEFAULT_SUPPORTED_FORMATS.to_vec(),
            SerializationFormat::Bincode,
        )
    }

    pub fn hello_with_formats(
        node_id: NodeId,
        supported_formats: Vec<SerializationFormat>,
        preferred_format: SerializationFormat,
    ) -> Self {
        Self {
            kind: MessageKind::Hello {
                node_id,
                protocol_version: PROTOCOL_VERSION,
                supported_formats,
                preferred_format,
            },
        }
    }

    pub fn hello_ack(node_id: NodeId) -> Self {
        Self::hello_ack_with_format(node_id, SerializationFormat::Bincode)
    }

    pub fn hello_ack_with_format(node_id: NodeId, selected_format: SerializationFormat) -> Self {
        Self {
            kind: MessageKind::HelloAck {
                node_id,
                protocol_version: PROTOCOL_VERSION,
                selected_format,
            },
        }
    }

    pub fn push_ops(ops: Vec<Op>) -> Self {
        Self {
            kind: MessageKind::PushOps { ops },
        }
    }

    pub fn push_ops_ack(received_count: usize) -> Self {
        Self {
            kind: MessageKind::PushOpsAck {
                received_count: received_count as u64,
            },
        }
    }

    pub fn pull_since(since_op_id: Option<String>) -> Self {
        Self {
            kind: MessageKind::PullSince { since_op_id },
        }
    }

    pub fn ping() -> Self {
        Self {
            kind: MessageKind::Ping,
        }
    }

    pub fn pong() -> Self {
        Self {
            kind: MessageKind::Pong,
        }
    }

    pub fn wire_error(error: WireError) -> Self {
        Self {
            kind: MessageKind::Error { error },
        }
    }

    /// Serialize to bytes using the default production wire format.
    pub fn to_bytes(&self) -> NetResult<Vec<u8>> {
        self.to_bytes_with_format(SerializationFormat::Bincode)
    }

    /// Serialize to bytes using the JSON debug wire format.
    pub fn to_json_bytes(&self) -> NetResult<Vec<u8>> {
        self.to_bytes_with_format(SerializationFormat::Json)
    }

    /// Serialize to bytes (length-prefixed format byte + payload).
    pub fn to_bytes_with_format(&self, format: SerializationFormat) -> NetResult<Vec<u8>> {
        let payload = match format {
            SerializationFormat::Json => serde_json::to_vec(self)?,
            SerializationFormat::Bincode => bincode::serialize(self)?,
        };
        let len = payload
            .len()
            .checked_add(1)
            .and_then(|len| u32::try_from(len).ok())
            .ok_or_else(|| NetError::InvalidMessage("message payload exceeds u32".to_string()))?;
        let len = len.to_be_bytes();
        let mut buf = Vec::with_capacity(4 + 1 + payload.len());
        buf.extend_from_slice(&len);
        buf.push(format.to_wire_byte());
        buf.extend_from_slice(&payload);
        Ok(buf)
    }

    /// Deserialize from bytes without the length prefix.
    pub fn from_bytes(bytes: &[u8]) -> NetResult<Self> {
        let (_, msg) = Self::from_bytes_with_format(bytes)?;
        Ok(msg)
    }

    /// Deserialize from bytes without the length prefix, returning the detected format.
    pub fn from_bytes_with_format(bytes: &[u8]) -> NetResult<(SerializationFormat, Self)> {
        let Some((&format_byte, payload)) = bytes.split_first() else {
            return Err(NetError::InvalidMessage(
                "message payload is missing serialization format byte".to_string(),
            ));
        };

        let format = SerializationFormat::from_wire_byte(format_byte)?;
        let msg = match format {
            SerializationFormat::Json => serde_json::from_slice(payload)?,
            SerializationFormat::Bincode => bincode::deserialize(payload)?,
        };
        Ok((format, msg))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hello_message() {
        let node_id = NodeId::new("test-node");
        let msg = Message::hello(node_id.clone());

        match &msg.kind {
            MessageKind::Hello {
                node_id: id,
                protocol_version,
                supported_formats,
                preferred_format,
            } => {
                assert_eq!(id, &node_id);
                assert_eq!(*protocol_version, PROTOCOL_VERSION);
                assert_eq!(supported_formats, DEFAULT_SUPPORTED_FORMATS);
                assert_eq!(*preferred_format, SerializationFormat::Bincode);
            }
            _ => panic!("wrong message kind"),
        }
    }

    #[test]
    fn hello_json_uses_explicit_protocol_version_field() {
        let value = serde_json::to_value(Message::hello(NodeId::new("test-node"))).unwrap();
        let hello = &value["kind"]["Hello"];

        assert_eq!(hello["protocol_version"], PROTOCOL_VERSION);
        assert!(hello.get("version").is_none());
    }

    #[test]
    fn test_message_roundtrip_json() {
        let node_id = NodeId::new("node-1");
        let msg = Message::hello(node_id);

        let bytes = msg.to_bytes_with_format(SerializationFormat::Json).unwrap();

        // Skip 4-byte length prefix
        let (format, parsed) = Message::from_bytes_with_format(&bytes[4..]).unwrap();

        assert_eq!(format, SerializationFormat::Json);
        assert_eq!(parsed, msg);
    }

    #[test]
    fn test_message_roundtrip_bincode() {
        let node = NodeId::new("node-1");
        let op = Op::gcounter_increment(node, "counter:test", 5);
        let msg = Message::push_ops(vec![op]);

        let bytes = msg
            .to_bytes_with_format(SerializationFormat::Bincode)
            .unwrap();

        // Skip 4-byte length prefix
        let (format, parsed) = Message::from_bytes_with_format(&bytes[4..]).unwrap();

        assert_eq!(format, SerializationFormat::Bincode);
        assert_eq!(parsed, msg);
    }

    #[test]
    fn rejects_unknown_serialization_format() {
        let err = Message::from_bytes(&[0xff, b'{', b'}']).unwrap_err();

        assert!(matches!(err, NetError::InvalidMessage(_)));
    }

    #[test]
    fn test_push_ops_message() {
        let node = NodeId::new("node-1");
        let op = Op::gcounter_increment(node, "counter:test", 5);
        let msg = Message::push_ops(vec![op]);

        match &msg.kind {
            MessageKind::PushOps { ops } => {
                assert_eq!(ops.len(), 1);
            }
            _ => panic!("wrong kind"),
        }
    }

    #[test]
    fn wire_error_roundtrips() {
        let msg = Message::wire_error(WireError::ProtocolMismatch {
            expected: PROTOCOL_VERSION,
            got: PROTOCOL_VERSION - 1,
        });

        let bytes = msg
            .to_bytes_with_format(SerializationFormat::Bincode)
            .unwrap();
        let (_, parsed) = Message::from_bytes_with_format(&bytes[4..]).unwrap();

        assert_eq!(parsed, msg);
    }

    #[test]
    fn wire_error_retry_policy_matches_semantics() {
        assert_eq!(
            WireError::ProtocolMismatch {
                expected: PROTOCOL_VERSION,
                got: PROTOCOL_VERSION - 1,
            }
            .retry_policy(),
            WireRetryPolicy::Fatal
        );
        assert_eq!(
            WireError::NotAuthorized {
                reason: "denied".into(),
            }
            .retry_policy(),
            WireRetryPolicy::Fatal
        );
        assert_eq!(
            WireError::RateLimited {
                retry_after_ms: Some(250),
            }
            .retry_policy(),
            WireRetryPolicy::RetryAfter(Duration::from_millis(250))
        );
        assert_eq!(
            WireError::RateLimited {
                retry_after_ms: None,
            }
            .retry_policy(),
            WireRetryPolicy::Retry
        );
        assert_eq!(
            WireError::Internal {
                reason: "temporary".into(),
            }
            .retry_policy(),
            WireRetryPolicy::Retry
        );
        assert_eq!(
            WireError::OpRejected {
                reason: "bad op".into(),
            }
            .retry_policy(),
            WireRetryPolicy::RequestFatal
        );
    }
}
