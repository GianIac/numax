//! The default build observes CRDT state without creating replication operations.
//! The `increment` build adds exactly one before observing it.
//! HTTP can read the local snapshot without accessing the reserved CRDT namespace.

use nx_sdk::{crdt::gcounter, db, net};

const COUNTER_KEY: &str = "discovery-lan:visits";
const SNAPSHOT_KEY: &str = "discovery-lan";

fn execute() -> nx_sdk::Result<()> {
    #[cfg(feature = "increment")]
    gcounter::inc(COUNTER_KEY, 1)?;

    let node_id = net::node_id()?;
    let value = gcounter::value(COUNTER_KEY)?;
    // This KV entry is local observation only; it is NOT the replicated counter.
    db::set(SNAPSHOT_KEY, format!("{node_id}\n{value}").as_bytes())?;
    Ok(())
}

#[unsafe(no_mangle)]
pub extern "C" fn run() {
    if execute().is_err() {
        nx_sdk::log("discovery_lan: SDK operation failed");
        // A failed SDK operation must fail the HTTP run, not look successful.
        #[cfg(target_arch = "wasm32")]
        core::arch::wasm32::unreachable();
        #[cfg(not(target_arch = "wasm32"))]
        panic!("discovery_lan is a WebAssembly guest");
    }
}
