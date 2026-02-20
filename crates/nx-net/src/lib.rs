//! # nx-net
//!
//! Networking peer-to-peer per Numax.
//! Gestisce la comunicazione tra nodi per la sincronizzazione CRDT.
//!
//! ## Protocollo
//! - `HELLO`: handshake iniziale con node_id e versione
//! - `PUSH_OPS`: invia operazioni CRDT ai peer
//! - `PULL_SINCE`: richiede operazioni mancanti (anti-entropy)
//!
//! ## Trasporto
//! TCP per ora. TODO: valutare QUIC per il futuro.

mod error;
mod message;
mod peer;
mod node;

pub use error::{NetError, NetResult};
pub use message::{Message, MessageKind};
pub use peer::{PeerId, PeerInfo};
pub use node::{Node, NodeConfig};   