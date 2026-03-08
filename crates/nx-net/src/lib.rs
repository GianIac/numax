//! # nx-net
//!
//! Peer-to-peer networking for Numax.
//! Handles communication between nodes for CRDT synchronization.
//!
//! ## Protocol
//! - `HELLO`: initial handshake with node_id and version
//! - `PUSH_OPS`: send CRDT operations to peers
//! - `PULL_SINCE`: request missing operations (anti-entropy)
//!
//! ## Transport
//! TCP with TLS 1.3 / mTLS for secure, authenticated connections.
//!
//! ## Security
//! - Mutual TLS: both peers authenticated
//! - NodeId = SHA256(pubkey): cryptographic identity
//! - Allowlist: optional permissioned network

mod error;
mod message;
mod node;
mod peer;
mod tls;

pub use error::{NetError, NetResult};
pub use message::{Message, MessageKind};
pub use node::{Node, NodeConfig, NodeEvent};
pub use peer::{PeerId, PeerInfo};
pub use tls::{
    NodeId, TestPki, TlsConfig, derive_node_id, generate_ca, generate_self_signed, generate_signed,
    node_id_from_hex, node_id_to_hex, write_cert_files,
};
