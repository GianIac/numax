//! Distributed Counter Example
//!
//! Questo modulo WASM incrementa un contatore replicato.
//! Quando eseguito su più nodi con sync abilitato,
//! il contatore converge allo stesso valore su tutti i nodi.
//!
//! ## Uso
//!
//! Terminale 1 (Nodo A):
//! ```bash
//! nx run distributed_counter.wasm \
//!     --listen 0.0.0.0:9000 \
//!     --sync-prefix "counter:" \
//!     --datastore-path ./data-a
//! ```
//!
//! Terminale 2 (Nodo B):
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

    // Leggi valore attuale
    let current = match db::get(COUNTER_KEY) {
        Ok(Some(bytes)) => {
            if bytes.len() >= 8 {
                let arr: [u8; 8] = bytes[..8].try_into().unwrap_or([0; 8]);
                u64::from_le_bytes(arr)
            } else {
                0
            }
        }
        Ok(None) => 0,  // Chiave non esiste
        Err(_) => 0,
    };

    log("Current value:");
    log(&current.to_string());

    // Incrementa
    let new_value = current + 1;

    // Salva
    let bytes = new_value.to_le_bytes();
    if let Err(_) = db::set(COUNTER_KEY, &bytes) {
        log("Error saving counter!");
        return;
    }

    log("New value:");
    log(&new_value.to_string());

    log("=== Done ===");
}