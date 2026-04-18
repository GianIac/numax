//! Distributed Counter Example
//!
//! This WASM module increments a replicated counter.
//! When run on multiple nodes with sync enabled,
//! the counter converges to the same value on all nodes.
//!
//! ## Usage
//!
//! Terminal 1 (Node A):
//! ```bash
//! nx run distributed_counter.wasm \
//!     --listen 0.0.0.0:9000 \
//!     --sync-prefix "counter:" \
//!     --datastore-path ./data-a
//! ```
//!
//! Terminal 2 (Node B):
//! ```bash
//! nx run distributed_counter.wasm \
//!     --listen 0.0.0.0:9001 \
//!     --peer 127.0.0.1:9000 \
//!     --sync-prefix "counter:" \
//!     --datastore-path ./data-b
//! ```

//! Distributed Counter Example

extern crate alloc;

use alloc::string::ToString;
use nx_sdk::{db, log};

const COUNTER_KEY: &str = "counter:visits";

#[no_mangle]
pub extern "C" fn run() {
    log("=== Distributed Counter ===");

    // Read current value
    let current = match db::get(COUNTER_KEY) {
        Ok(Some(bytes)) => {
            if bytes.len() >= 8 {
                let arr: [u8; 8] = bytes[..8].try_into().unwrap_or([0; 8]);
                u64::from_le_bytes(arr)
            } else {
                0
            }
        }
        Ok(None) => 0,  // Key does not exist
        Err(_) => 0,
    };

    log("Current value:");
    log(&current.to_string());

    // Increment
    let new_value = current + 1;

    // Save
    let bytes = new_value.to_le_bytes();
    if let Err(_) = db::set(COUNTER_KEY, &bytes) {
        log("Error saving counter!");
        return;
    }

    log("New value:");
    log(&new_value.to_string());

    log("=== Done ===");
}