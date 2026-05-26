//! Distributed Settings Example.
//!
//! An LWW-Map-backed service settings document replicated across Numax nodes.
//! Each run applies one field action selected by `NX_SETTING_ACTION`.

extern crate alloc;

use alloc::string::{String, ToString};
use alloc::vec::Vec;

use nx_sdk::crdt::lww_map;
use nx_sdk::{NxError, net, nx_log, system};

const DEFAULT_SETTINGS_KEY: &str = "settings:service-a";
const DEFAULT_SETTING_FIELD: &str = "theme";
const DEFAULT_SETTING_VALUE: &str = "dark";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SettingAction {
    Set,
    Remove,
    List,
}

impl SettingAction {
    fn from_env() -> Self {
        match system::env_get("NX_SETTING_ACTION").ok().flatten().as_deref() {
            Some(b"remove") => Self::Remove,
            Some(b"list") => Self::List,
            _ => Self::Set,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Set => "set",
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

fn display_bytes(bytes: &[u8]) -> &str {
    core::str::from_utf8(bytes).unwrap_or("<non-utf8>")
}

fn format_entries(entries: &[(String, Vec<u8>)]) -> String {
    if entries.is_empty() {
        "{}".to_string()
    } else {
        let fields = entries
            .iter()
            .map(|(field, value)| alloc::format!("{field}={}", display_bytes(value)))
            .collect::<Vec<_>>();
        alloc::format!("{{{}}}", fields.join(", "))
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn run() {
    nx_log!("distributed_settings: start");

    let module_id = match system::module_id() {
        Ok(module_id) => module_id,
        Err(e) => {
            nx_log!("distributed_settings: failed to read module id: {}", e);
            return;
        }
    };
    nx_log!("distributed_settings: module_id={}", module_id);

    match net::node_id() {
        Ok(node_id) => nx_log!("distributed_settings: node_id={}", node_id),
        Err(NxError::SyncDisabled) => {
            nx_log!("distributed_settings: sync disabled");
            nx_log!("distributed_settings: run with --listen <addr>");
            return;
        }
        Err(e) => {
            nx_log!("distributed_settings: failed to read node id: {}", e);
            return;
        }
    }

    let key = env_string("NX_SETTING_KEY", DEFAULT_SETTINGS_KEY);
    let field = env_string("NX_SETTING_FIELD", DEFAULT_SETTING_FIELD);
    let value = env_string("NX_SETTING_VALUE", DEFAULT_SETTING_VALUE);
    let action = SettingAction::from_env();

    match lww_map::entries(&key) {
        Ok(entries) => nx_log!(
            "distributed_settings: {} before={}",
            key,
            format_entries(&entries)
        ),
        Err(e) => {
            nx_log!("distributed_settings: failed to read starting settings: {}", e);
            return;
        }
    }

    let result = match action {
        SettingAction::Set => lww_map::set(&key, &field, value.as_bytes()),
        SettingAction::Remove => lww_map::remove(&key, &field),
        SettingAction::List => Ok(()),
    };

    if let Err(e) = result {
        nx_log!(
            "distributed_settings: failed to apply action={} field={}: {}",
            action.label(),
            field,
            e
        );
        return;
    }

    match lww_map::get(&key, &field) {
        Ok(Some(current)) => nx_log!(
            "distributed_settings: action={} field={} value={}",
            action.label(),
            field,
            display_bytes(&current)
        ),
        Ok(None) => nx_log!(
            "distributed_settings: action={} field={} value=<unset>",
            action.label(),
            field
        ),
        Err(e) => {
            nx_log!("distributed_settings: failed to read field: {}", e);
            return;
        }
    }

    match lww_map::contains(&key, &field) {
        Ok(contains) => nx_log!(
            "distributed_settings: field={} contains={}",
            field,
            contains
        ),
        Err(e) => {
            nx_log!("distributed_settings: failed to check field visibility: {}", e);
            return;
        }
    }

    match lww_map::entries(&key) {
        Ok(entries) => nx_log!(
            "distributed_settings: {} local_after={}",
            key,
            format_entries(&entries)
        ),
        Err(e) => {
            nx_log!("distributed_settings: failed to read final settings: {}", e);
            return;
        }
    }

    match net::peers() {
        Ok(peers) => nx_log!("distributed_settings: connected_peers={}", peers.len()),
        Err(e) => nx_log!("distributed_settings: failed to inspect peers: {}", e),
    }

    nx_log!("distributed_settings: done");
}
