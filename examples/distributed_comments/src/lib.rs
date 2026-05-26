//! Distributed Comments Example.
//!
//! An RGA-backed ordered comment stream replicated across Numax nodes. Each
//! run appends, inserts after a known element id, deletes, or lists visible
//! comments selected by environment variables.

extern crate alloc;

use alloc::string::{String, ToString};

use nx_sdk::crdt::rga;
use nx_sdk::{NxError, net, nx_log, system};

const DEFAULT_COMMENTS_KEY: &str = "comments:doc-1";
const DEFAULT_COMMENT_TEXT: &str = "hello from numax";

fn env_string(name: &str, default: &str) -> String {
    match system::env_get(name).ok().flatten() {
        Some(bytes) => String::from_utf8(bytes).unwrap_or_else(|_| default.to_string()),
        None => default.to_string(),
    }
}

fn env_optional_string(name: &str) -> Option<String> {
    system::env_get(name)
        .ok()
        .flatten()
        .and_then(|bytes| String::from_utf8(bytes).ok())
        .filter(|value| !value.is_empty())
}

fn display_bytes(bytes: &[u8]) -> &str {
    core::str::from_utf8(bytes).unwrap_or("<non-utf8>")
}

fn log_values(key: &str) {
    match rga::values(key) {
        Ok(values) => {
            nx_log!("distributed_comments: {} visible_count={}", key, values.len());
            for (index, value) in values.iter().enumerate() {
                nx_log!(
                    "distributed_comments: {}[{}]={}",
                    key,
                    index,
                    display_bytes(value)
                );
            }
        }
        Err(e) => nx_log!("distributed_comments: failed to list {}: {}", key, e),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn run() {
    nx_log!("distributed_comments: start");

    match net::node_id() {
        Ok(node_id) => nx_log!("distributed_comments: node_id={}", node_id),
        Err(NxError::SyncDisabled) => {
            nx_log!("distributed_comments: sync disabled");
            nx_log!("distributed_comments: run with --listen <addr>");
            return;
        }
        Err(e) => {
            nx_log!("distributed_comments: failed to read node id: {}", e);
            return;
        }
    }

    let key = env_string("NX_COMMENT_KEY", DEFAULT_COMMENTS_KEY);
    let action = env_string("NX_COMMENT_ACTION", "append");
    log_values(&key);

    match action.as_str() {
        "append" | "insert" => {
            let parent = env_optional_string("NX_COMMENT_PARENT");
            let text = env_string("NX_COMMENT_TEXT", DEFAULT_COMMENT_TEXT);
            match rga::insert_after(&key, parent.as_deref(), text.as_bytes()) {
                Ok(id) => {
                    nx_log!(
                        "distributed_comments: inserted id={} parent={} text={}",
                        id,
                        parent.as_deref().unwrap_or("<head>"),
                        text
                    );
                }
                Err(e) => {
                    nx_log!("distributed_comments: insert failed: {}", e);
                    return;
                }
            }
        }
        "delete" => {
            let Some(id) = env_optional_string("NX_COMMENT_ID") else {
                nx_log!("distributed_comments: delete requires NX_COMMENT_ID");
                return;
            };
            if let Err(e) = rga::delete(&key, &id) {
                nx_log!("distributed_comments: delete failed for {}: {}", id, e);
                return;
            }
            nx_log!("distributed_comments: deleted id={}", id);
        }
        "list" => {}
        other => {
            nx_log!(
                "distributed_comments: unsupported NX_COMMENT_ACTION={} (use append, insert, delete, list)",
                other
            );
            return;
        }
    }

    log_values(&key);

    match net::peers() {
        Ok(peers) => nx_log!("distributed_comments: connected_peers={}", peers.len()),
        Err(e) => nx_log!("distributed_comments: failed to inspect peers: {}", e),
    }

    nx_log!("distributed_comments: done");
}
