pub mod crdt;
mod error;
mod node_id;
mod op;

pub use error::{SyncError, SyncResult};
pub use node_id::NodeId;
pub use op::{Op, OpId, OpKind};  // <-- Aggiunto OpKind!

// Re-export CRDT types
pub use crdt::gcounter::GCounter;