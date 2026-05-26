pub mod crdt;
mod error;
mod node_id;
mod op;

pub use error::{SyncError, SyncResult};
pub use node_id::NodeId;
pub use op::{Op, OpId, OpKind};

pub use crdt::gcounter::GCounter;
pub use crdt::lww_register::LwwRegister;
pub use crdt::pncounter::PNCounter;
