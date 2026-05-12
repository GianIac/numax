//! Vote Tally TLS Example.
//!
//! A tiny replicated vote tally backed by Numax's GCounter CRDT. Each run casts
//! one vote for the fixed "yes" tally and prints the local value before and
//! after the increment. Transport security and peer admission are configured by
//! the host through mTLS and `--allowed-peers`.

extern crate alloc;

use alloc::string::ToString;
use nx_sdk::crdt::gcounter;
use nx_sdk::{NxError, log};

const TALLY_KEY: &str = "vote:tally:yes";

#[unsafe(no_mangle)]
pub extern "C" fn run() {
    log("=== Vote Tally (mTLS + GCounter) ===");

    match gcounter::value(TALLY_KEY) {
        Ok(v) => {
            log("Votes before local cast:");
            log(&v.to_string());
        }
        Err(NxError::SyncDisabled) => {
            log("sync is disabled on this runtime.");
            log("start the runtime with --listen <addr> and TLS flags.");
            return;
        }
        Err(e) => {
            log("error reading vote tally:");
            log(&e.to_string());
            return;
        }
    }

    if let Err(e) = gcounter::inc(TALLY_KEY, 1) {
        log("error casting vote:");
        log(&e.to_string());
        return;
    }

    match gcounter::value(TALLY_KEY) {
        Ok(v) => {
            log("Votes after local cast:");
            log(&v.to_string());
        }
        Err(e) => {
            log("error reading vote tally after cast:");
            log(&e.to_string());
        }
    }

    log("=== Done ===");
}
