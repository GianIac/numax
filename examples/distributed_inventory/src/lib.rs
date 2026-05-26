//! Distributed Inventory Example.
//!
//! A PNCounter-backed stock value replicated across Numax nodes. Each run
//! applies a deterministic operation selected by `NX_INVENTORY_ACTION`.

extern crate alloc;

use nx_sdk::crdt::pncounter;
use nx_sdk::{NxError, net, nx_log, system};

const SKU_KEY: &str = "inventory:sku-1";

#[derive(Debug, Clone, Copy)]
enum InventoryAction {
    Restock,
    Sale,
    Return,
}

impl InventoryAction {
    fn from_env() -> Self {
        match system::env_get("NX_INVENTORY_ACTION")
            .ok()
            .flatten()
            .as_deref()
        {
            Some(b"sale") => Self::Sale,
            Some(b"return") => Self::Return,
            _ => Self::Restock,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Restock => "restock",
            Self::Sale => "sale",
            Self::Return => "return",
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn run() {
    nx_log!("distributed_inventory: start");

    let node_id = match system::module_id() {
        Ok(module_id) => module_id,
        Err(e) => {
            nx_log!("distributed_inventory: failed to read module id: {}", e);
            return;
        }
    };
    nx_log!("distributed_inventory: module_id={}", node_id);

    let before = match pncounter::value(SKU_KEY) {
        Ok(value) => value,
        Err(NxError::SyncDisabled) => {
            nx_log!("distributed_inventory: sync disabled");
            nx_log!("distributed_inventory: run with --listen <addr>");
            return;
        }
        Err(e) => {
            nx_log!(
                "distributed_inventory: failed to read starting stock: {}",
                e
            );
            return;
        }
    };
    nx_log!("distributed_inventory: {} before={}", SKU_KEY, before);

    let action = InventoryAction::from_env();
    let result = match action {
        InventoryAction::Restock => pncounter::inc(SKU_KEY, 10),
        InventoryAction::Sale => pncounter::dec(SKU_KEY, 4),
        InventoryAction::Return => pncounter::inc(SKU_KEY, 2),
    };

    if let Err(e) = result {
        nx_log!(
            "distributed_inventory: failed to apply {} action: {}",
            action.label(),
            e
        );
        return;
    }

    match pncounter::value(SKU_KEY) {
        Ok(after) => {
            nx_log!(
                "distributed_inventory: action={} local_after={}",
                action.label(),
                after
            );
        }
        Err(e) => {
            nx_log!(
                "distributed_inventory: failed to read final local stock: {}",
                e
            );
            return;
        }
    }

    match net::peers() {
        Ok(peers) => nx_log!("distributed_inventory: connected_peers={}", peers.len()),
        Err(e) => nx_log!("distributed_inventory: failed to inspect peers: {}", e),
    }

    nx_log!("distributed_inventory: done");
}
