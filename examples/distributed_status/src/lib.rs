//! Distributed Status Example.
//!
//! An LWW-Register-backed service status replicated across Numax nodes. Each
//! run writes one local status selected by `NX_STATUS_VALUE`.

extern crate alloc;

use alloc::string::{String, ToString};

use nx_sdk::crdt::lww_register;
use nx_sdk::{NxError, net, nx_log, system};

const DEFAULT_STATUS_KEY: &str = "status:service-a";
const DEFAULT_STATUS_VALUE: &str = "online";

fn env_string(name: &str, default: &str) -> String {
    match system::env_get(name).ok().flatten() {
        Some(bytes) => String::from_utf8(bytes).unwrap_or_else(|_| default.to_string()),
        None => default.to_string(),
    }
}

fn display_bytes(bytes: &[u8]) -> &str {
    core::str::from_utf8(bytes).unwrap_or("<non-utf8>")
}

#[unsafe(no_mangle)]
pub extern "C" fn run() {
    nx_log!("distributed_status: start");

    let module_id = match system::module_id() {
        Ok(module_id) => module_id,
        Err(e) => {
            nx_log!("distributed_status: failed to read module id: {}", e);
            return;
        }
    };
    nx_log!("distributed_status: module_id={}", module_id);

    match net::node_id() {
        Ok(node_id) => nx_log!("distributed_status: node_id={}", node_id),
        Err(NxError::SyncDisabled) => {
            nx_log!("distributed_status: sync disabled");
            nx_log!("distributed_status: run with --listen <addr>");
            return;
        }
        Err(e) => {
            nx_log!("distributed_status: failed to read node id: {}", e);
            return;
        }
    }

    let key = env_string("NX_STATUS_KEY", DEFAULT_STATUS_KEY);
    let next_status = env_string("NX_STATUS_VALUE", DEFAULT_STATUS_VALUE);

    match lww_register::get(&key) {
        Ok(Some(value)) => {
            nx_log!(
                "distributed_status: {} before={}",
                key,
                display_bytes(&value)
            );
        }
        Ok(None) => nx_log!("distributed_status: {} before=<unset>", key),
        Err(e) => {
            nx_log!("distributed_status: failed to read starting status: {}", e);
            return;
        }
    }

    if let Err(e) = lww_register::set(&key, next_status.as_bytes()) {
        nx_log!(
            "distributed_status: failed to set {} to {}: {}",
            key,
            next_status,
            e
        );
        return;
    }

    match lww_register::get(&key) {
        Ok(Some(value)) => {
            nx_log!(
                "distributed_status: wrote={} local_after={}",
                next_status,
                display_bytes(&value)
            );
        }
        Ok(None) => nx_log!("distributed_status: local_after=<unset>"),
        Err(e) => {
            nx_log!("distributed_status: failed to read final local status: {}", e);
            return;
        }
    }

    match net::peers() {
        Ok(peers) => nx_log!("distributed_status: connected_peers={}", peers.len()),
        Err(e) => nx_log!("distributed_status: failed to inspect peers: {}", e),
    }

    nx_log!("distributed_status: done");
}
