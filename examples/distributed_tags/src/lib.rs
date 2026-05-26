//! Distributed Tags Example.
//!
//! An ORSet-backed tag set replicated across Numax nodes. Each run applies one
//! local tag action selected by `NX_TAG_ACTION`.

extern crate alloc;

use alloc::string::{String, ToString};

use nx_sdk::crdt::orset;
use nx_sdk::{NxError, net, nx_log, system};

const DEFAULT_TAG_KEY: &str = "tags:doc-1";
const DEFAULT_TAG_VALUE: &str = "urgent";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TagAction {
    Add,
    Remove,
    List,
}

impl TagAction {
    fn from_env() -> Self {
        match system::env_get("NX_TAG_ACTION").ok().flatten().as_deref() {
            Some(b"remove") => Self::Remove,
            Some(b"list") => Self::List,
            _ => Self::Add,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Add => "add",
            Self::Remove => "remove",
            Self::List => "list",
        }
    }
}

fn env_string(name: &str, default: &str) -> String {
    match system::env_get(name).ok().flatten() {
        Some(bytes) => String::from_utf8(bytes).unwrap_or_else(|_| default.to_string()),
        None => default.to_string(),
    }
}

fn format_elements(elements: &[String]) -> String {
    if elements.is_empty() {
        "[]".to_string()
    } else {
        alloc::format!("[{}]", elements.join(", "))
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn run() {
    nx_log!("distributed_tags: start");

    let module_id = match system::module_id() {
        Ok(module_id) => module_id,
        Err(e) => {
            nx_log!("distributed_tags: failed to read module id: {}", e);
            return;
        }
    };
    nx_log!("distributed_tags: module_id={}", module_id);

    match net::node_id() {
        Ok(node_id) => nx_log!("distributed_tags: node_id={}", node_id),
        Err(NxError::SyncDisabled) => {
            nx_log!("distributed_tags: sync disabled");
            nx_log!("distributed_tags: run with --listen <addr>");
            return;
        }
        Err(e) => {
            nx_log!("distributed_tags: failed to read node id: {}", e);
            return;
        }
    }

    let key = env_string("NX_TAG_KEY", DEFAULT_TAG_KEY);
    let tag = env_string("NX_TAG_VALUE", DEFAULT_TAG_VALUE);
    let action = TagAction::from_env();

    match orset::elements(&key) {
        Ok(elements) => {
            nx_log!(
                "distributed_tags: {} before={}",
                key,
                format_elements(&elements)
            );
        }
        Err(e) => {
            nx_log!("distributed_tags: failed to read starting tags: {}", e);
            return;
        }
    }

    let result = match action {
        TagAction::Add => orset::add(&key, &tag),
        TagAction::Remove => orset::remove(&key, &tag),
        TagAction::List => Ok(()),
    };

    if let Err(e) = result {
        nx_log!(
            "distributed_tags: failed to apply action={} tag={}: {}",
            action.label(),
            tag,
            e
        );
        return;
    }

    match orset::contains(&key, &tag) {
        Ok(contains) => nx_log!(
            "distributed_tags: action={} tag={} contains={}",
            action.label(),
            tag,
            contains
        ),
        Err(e) => {
            nx_log!("distributed_tags: failed to check tag visibility: {}", e);
            return;
        }
    }

    match orset::elements(&key) {
        Ok(elements) => {
            nx_log!(
                "distributed_tags: {} local_after={}",
                key,
                format_elements(&elements)
            );
        }
        Err(e) => {
            nx_log!("distributed_tags: failed to read final local tags: {}", e);
            return;
        }
    }

    match net::peers() {
        Ok(peers) => nx_log!("distributed_tags: connected_peers={}", peers.len()),
        Err(e) => nx_log!("distributed_tags: failed to inspect peers: {}", e),
    }

    nx_log!("distributed_tags: done");
}
