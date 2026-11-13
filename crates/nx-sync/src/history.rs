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

/// Bounded history exchange. Limits are validated by the consumer.
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
    BaselineRequest {
        request_id: crate::OpId,
        origin: NodeId,
        version: u16,
    },
    Baseline {
        request_id: crate::OpId,
        origin: NodeId,
        version: u16,
        payload: Vec<u8>,
        digest: [u8; 32],
    },
    BaselineUnavailable {
        request_id: crate::OpId,
        origin: NodeId,
        reason: BaselineFailure,
    },
}

/// Source-scoped failures: none proves that every replica has lost its state.
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
pub enum BaselineFailure {
    UnsupportedVersion,
    SourceIncomplete,
    BudgetExceeded,
    CoverageConflict,
    InvalidState,
}

/// A complete cut, including any unsequenced historical baseline it represents.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BaselineOrigin {
    pub origin: NodeId,
    pub sequence: u64,
    pub legacy: bool,
}

/// Version 1 carries the existing, validated full-state JSON for each family.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BaselineRecord {
    /// 1=GCounter, 2=PNCounter, 3=LwwRegister, 4=LwwMap, 5=ORSet, 6=Rga.
    pub family: u8,
    pub key: String,
    pub state: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CrdtBaseline {
    pub origins: Vec<BaselineOrigin>,
    pub records: Vec<BaselineRecord>,
}
