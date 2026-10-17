use serde::{Deserialize, Serialize};

use crate::{NodeId, Op};

/// Durable position assigned by the origin. Sequence numbers continue across restarts.
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
pub struct OpPosition {
    pub generation: u64,
    pub sequence: u64,
}

/// Replication envelope; the underlying operation and its UUID remain unchanged.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, wincode::SchemaRead, wincode::SchemaWrite,
)]
pub struct SequencedOp {
    pub op: Op,
    pub position: OpPosition,
}

/// Known history, not a receipt frontier or a durability acknowledgement.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, wincode::SchemaRead, wincode::SchemaWrite,
)]
pub struct HistorySummary {
    /// This source retains a legacy baseline outside the sequenced history.
    pub baseline_required: bool,
    pub origin: NodeId,
    pub high_sequence: u64,
}

/// Bounded history exchange used by protocol 7. Limits are validated by the consumer.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, wincode::SchemaRead, wincode::SchemaWrite,
)]
pub enum HistoryMessage {
    SummaryRequest {
        after_origin: Option<String>,
    },
    Push {
        ops: Vec<SequencedOp>,
    },
    Summary {
        entries: Vec<HistorySummary>,
    },
    Request {
        request_id: crate::OpId,
        origin: NodeId,
        start: u64,
        end: u64,
    },
    Replay {
        request_id: crate::OpId,
        origin: NodeId,
        start: u64,
        end: u64,
        ops: Vec<SequencedOp>,
    },
    Unavailable {
        request_id: crate::OpId,
        origin: NodeId,
        start: u64,
        end: u64,
    },
}
