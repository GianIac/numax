use std::time::Duration;

use crate::{NetError, NetResult};
use nx_sync::{NodeId, Op};
use serde::{Deserialize, Serialize};

/// Protocol version.
pub const PROTOCOL_VERSION: u32 = 4;

const FORMAT_JSON: u8 = 0x01;
const FORMAT_BINCODE: u8 = 0x02;
const MIB: usize = 1024 * 1024;

/// Wire payload serialization format.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    wincode::SchemaRead,
    wincode::SchemaWrite,
)]
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
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, wincode::SchemaRead, wincode::SchemaWrite,
)]
pub enum WireError {
    ProtocolMismatch { expected: u32, got: u32 },
    OpRejected { reason: String },
    RateLimited { retry_after_ms: Option<u64> },
    NotAuthorized { reason: String },
    Internal { reason: String },
}

/// Reconnect behavior implied by a structured wire error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
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
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, wincode::SchemaRead, wincode::SchemaWrite,
)]
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
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, wincode::SchemaRead, wincode::SchemaWrite,
)]
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
            SerializationFormat::Bincode => wincode::config::serialize(
                self,
                wincode::config::Configuration::default().disable_preallocation_size_limit(),
            )?,
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
            SerializationFormat::Bincode => deserialize_binary(payload)?,
        };
        Ok((format, msg))
    }
}

pub(crate) fn validate_payload_len(len: usize, limit: usize) -> NetResult<()> {
    if len > limit {
        return Err(NetError::MessageTooLarge { len, limit });
    }
    Ok(())
}

fn deserialize_binary(payload: &[u8]) -> Result<Message, wincode::ReadError> {
    fn with_limit<const LIMIT: usize>(payload: &[u8]) -> Result<Message, wincode::ReadError> {
        wincode::config::deserialize_exact(
            payload,
            wincode::config::Configuration::default().with_preallocation_size_limit::<LIMIT>(),
        )
    }

    if payload.len() <= 4 * MIB {
        with_limit::<{ 4 * MIB }>(payload)
    } else if payload.len() <= 16 * MIB {
        with_limit::<{ 16 * MIB }>(payload)
    } else if payload.len() <= 64 * MIB {
        with_limit::<{ 64 * MIB }>(payload)
    } else if payload.len() <= 256 * MIB {
        with_limit::<{ 256 * MIB }>(payload)
    } else if payload.len() <= 1024 * MIB {
        with_limit::<{ 1024 * MIB }>(payload)
    } else {
        wincode::config::deserialize_exact(
            payload,
            wincode::config::Configuration::default().disable_preallocation_size_limit(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nx_sync::{OpId, OpKind};

    fn protocol_v4_messages() -> Vec<Message> {
        let origin = NodeId::new("node-a");
        let ops = vec![
            Op {
                id: OpId::new("op-1"),
                origin: origin.clone(),
                kind: OpKind::GCounterIncrement {
                    key: "counter".into(),
                    increment: 7,
                },
            },
            Op {
                id: OpId::new("op-2"),
                origin: origin.clone(),
                kind: OpKind::PNCounterIncrement {
                    key: "inventory".into(),
                    increment: 3,
                },
            },
            Op {
                id: OpId::new("op-3"),
                origin: origin.clone(),
                kind: OpKind::PNCounterDecrement {
                    key: "inventory".into(),
                    decrement: 2,
                },
            },
            Op {
                id: OpId::new("op-4"),
                origin: origin.clone(),
                kind: OpKind::LwwRegisterSet {
                    key: "status".into(),
                    value: b"ready".to_vec(),
                    timestamp_ms: 42,
                },
            },
            Op {
                id: OpId::new("op-5"),
                origin: origin.clone(),
                kind: OpKind::LwwMapSet {
                    key: "settings".into(),
                    field: "theme".into(),
                    value: b"dark".to_vec(),
                    timestamp_ms: 43,
                },
            },
            Op {
                id: OpId::new("op-6"),
                origin: origin.clone(),
                kind: OpKind::LwwMapRemove {
                    key: "settings".into(),
                    field: "theme".into(),
                    timestamp_ms: 44,
                },
            },
            Op {
                id: OpId::new("op-7"),
                origin: origin.clone(),
                kind: OpKind::ORSetAdd {
                    key: "tags".into(),
                    element: "rust".into(),
                    tag: "tag-1".into(),
                },
            },
            Op {
                id: OpId::new("op-8"),
                origin: origin.clone(),
                kind: OpKind::ORSetRemove {
                    key: "tags".into(),
                    element: "rust".into(),
                    observed_tags: vec!["tag-1".into()],
                },
            },
            Op {
                id: OpId::new("op-9"),
                origin: origin.clone(),
                kind: OpKind::RgaInsert {
                    key: "comments".into(),
                    id: "item-1".into(),
                    parent: None,
                    value: b"hello".to_vec(),
                },
            },
            Op {
                id: OpId::new("op-10"),
                origin: origin.clone(),
                kind: OpKind::RgaDelete {
                    key: "comments".into(),
                    id: "item-1".into(),
                },
            },
        ];

        vec![
            Message::hello(origin.clone()),
            Message::hello_ack(origin),
            Message::push_ops(ops),
            Message::push_ops_ack(10),
            Message::pull_since(None),
            Message::pull_since(Some("op-10".into())),
            Message::ping(),
            Message::pong(),
            Message::wire_error(WireError::ProtocolMismatch {
                expected: PROTOCOL_VERSION,
                got: PROTOCOL_VERSION - 1,
            }),
            Message::wire_error(WireError::OpRejected {
                reason: "invalid op".into(),
            }),
            Message::wire_error(WireError::RateLimited {
                retry_after_ms: Some(500),
            }),
            Message::wire_error(WireError::NotAuthorized {
                reason: "peer denied".into(),
            }),
            Message::wire_error(WireError::Internal {
                reason: "internal".into(),
            }),
        ]
    }

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
    fn protocol_v4_binary_encoding_matches_bincode_golden_hashes() {
        let expected_sha256 = [
            "62cb7aa9f8be207d22c1b8e92bdf8096ddc4e1f1ed79a64b7e42047ae267df9a",
            "762558e92347d927b302e4a5a22de6a7f61feb74b25108d1adbe0037b93463f8",
            "1953b5c9bfa1929dbe636c27e4e6d504d585c2eba0eb4f61d5a955974b57c31d",
            "7c16f5631b09eef6cfc2ecdfb0d5336adbaa187c45cf7b6c5e37c4b6dc98158d",
            "88420266dfd64d604627234a8a6c75cf6477c6fd5505df0d17c59959ae9ce234",
            "0dd60804260500069dbc38d3b7f3cc4c54ae6952e89b620a9c6d7378705e5b78",
            "2594b6a92ebfb1c3312deb7d01c015fb95e9fbe9bd7bc6b527af07813ec7b910",
            "7aa8ca4a02506da9133d8f889678b76f716ce45d02e22fdb7b70a15e56a0eff8",
            "4779c171ec57c753c34e20aa6a17595fb121d7bea35261f990213a495ef9cca5",
            "0239a8fac27cbe2066f549e3ef3bf654f34699e7338f328878dbdb5a956096ee",
            "169f3c91969ead0a7a678f98088e54519e7c8679ed6d8a5ade85d7a00c718e50",
            "678ff351757c2bbcba3d3aeb9aa6cef34c34dd07b122817509765742351ec3ab",
            "574f81f9e34c4b5f8d195759d62c42983380a5a83ddd77cfebe5e7dd84425ae0",
        ];
        let messages = protocol_v4_messages();
        assert_eq!(messages.len(), expected_sha256.len());

        for (message, expected_hash) in messages.into_iter().zip(expected_sha256) {
            let bytes = wincode::serialize(&message).unwrap();
            let actual_hash = hex::encode(<sha2::Sha256 as sha2::Digest>::digest(&bytes));
            assert_eq!(actual_hash, expected_hash, "message: {message:?}");

            let decoded: Message = wincode::deserialize_exact(&bytes).unwrap();
            assert_eq!(decoded, message);
        }
    }

    #[test]
    fn bincode_roundtrip_supports_payloads_above_wincode_default_limit() {
        let message = Message::push_ops(vec![Op {
            id: OpId::new("large-op"),
            origin: NodeId::new("node-a"),
            kind: OpKind::LwwRegisterSet {
                key: "large-value".into(),
                value: vec![0x5a; 5 * MIB],
                timestamp_ms: 42,
            },
        }]);

        let bytes = message
            .to_bytes_with_format(SerializationFormat::Bincode)
            .unwrap();
        let decoded = Message::from_bytes(&bytes[4..]).unwrap();
        assert_eq!(decoded, message);
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
