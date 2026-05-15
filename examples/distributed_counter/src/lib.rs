//! Distributed Counter Example — CRDT edition.
//!
//! A grow-only counter (GCounter) shared across nodes. Each `run` increments
//! the counter by 1 on the local node; the increment is broadcast to peers
//! and the counter converges across all nodes thanks to the CRDT merge
//! semantics (take-max per slot, sum across slots).
//!
//! ## Usage
//!
//! Terminal 1 (Node A):
//! ```bash
//! nx run distributed_counter.wasm \
//!     --listen 0.0.0.0:9000 \
//!     --peer 127.0.0.1:9001 \
//!     --datastore-path ./data-a \
//!     --wait-before-run 1500ms \
//!     --settle-for 2s \
//!     --print-gcounter counter:visits
//! ```
//!
//! Terminal 2 (Node B):
//! ```bash
//! nx run distributed_counter.wasm \
//!     --listen 0.0.0.0:9001 \
//!     --peer 127.0.0.1:9000 \
//!     --datastore-path ./data-b \
//!     --wait-before-run 1500ms \
//!     --settle-for 2s \
//!     --print-gcounter counter:visits
//! ```

extern crate alloc;

use alloc::string::ToString;
use nx_sdk::crdt::gcounter;
use nx_sdk::{NxError, log};

const COUNTER_KEY: &str = "counter:visits";

#[unsafe(no_mangle)]
pub extern "C" fn run() {
    log("=== Distributed Counter (CRDT) ===");

    match gcounter::value(COUNTER_KEY) {
        Ok(v) => {
            log("Current value:");
            log(&v.to_string());
        }
        Err(NxError::SyncDisabled) => {
            log("sync is disabled on this runtime.");
            log("start the runtime with --listen <addr> to enable CRDT replication.");
            return;
        }
        Err(e) => {
            log("error reading counter:");
            log(&e.to_string());
            return;
        }
    }

    if let Err(e) = gcounter::inc(COUNTER_KEY, 1) {
        log("error incrementing counter:");
        log(&e.to_string());
        return;
    }

    match gcounter::value(COUNTER_KEY) {
        Ok(v) => {
            log("New value:");
            log(&v.to_string());
        }
        Err(e) => {
            log("error reading counter after increment:");
            log(&e.to_string());
        }
    }

    log("=== Done ===");
}
