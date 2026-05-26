use std::env;

use anyhow::Result;
use wasmtime::{Caller, Error, Linker, Memory};

use crate::runtime::HostState;

const ERR_NOT_FOUND: i32 = -1;
const ERR_BUF_TOO_SMALL: i32 = -2;
const ERR_INTERNAL: i32 = -3;

const MAX_ENV_KEY_LEN: u32 = 128;
const MAX_EVENT_NAME_LEN: u32 = 128;
const MAX_EVENT_PAYLOAD_LEN: u32 = 64 * 1024;
const MAX_ABORT_MSG_LEN: u32 = 8 * 1024;
const MAX_OUT_CAP: u32 = 1024 * 1024;

const HOST_CAPABILITIES: &[&str] = &[
    "abort",
    "crdt_gcounter_inc",
    "crdt_gcounter_value",
    "crdt_lww_get",
    "crdt_lww_set",
    "crdt_pncounter_dec",
    "crdt_pncounter_inc",
    "crdt_pncounter_value",
    "db_delete",
    "db_exists",
    "db_get",
    "db_keys",
    "db_keys_after",
    "db_scan",
    "db_scan_after",
    "db_set",
    "env_get",
    "event_emit",
    "hash_blake3",
    "hash_sha256",
    "host_capabilities",
    "host_log",
    "host_log_v2",
    "module_id",
    "net_node_id",
    "net_peers",
    "random_bytes",
    "time_monotonic",
    "time_now",
];

fn get_memory(caller: &mut Caller<'_, HostState>) -> Option<Memory> {
    match caller.get_export("memory") {
        Some(wasmtime::Extern::Memory(mem)) => Some(mem),
        _ => None,
    }
}

fn read_bytes(
    caller: &mut Caller<'_, HostState>,
    memory: &Memory,
    ptr: u32,
    len: u32,
    max_len: u32,
) -> Result<Vec<u8>> {
    if len > max_len {
        anyhow::bail!("requested length too large: {len} > {max_len}");
    }

    let mut buf = vec![0u8; len as usize];
    memory.read(caller, ptr as usize, &mut buf)?;
    Ok(buf)
}

fn write_bytes(
    caller: &mut Caller<'_, HostState>,
    memory: &Memory,
    out_ptr: u32,
    out_cap: u32,
    bytes: &[u8],
    api_name: &str,
) -> i32 {
    if out_cap > MAX_OUT_CAP {
        eprintln!("[nx-core] {api_name}: output capacity too large: {out_cap} (max {MAX_OUT_CAP})");
        return ERR_INTERNAL;
    }
    if bytes.len() > out_cap as usize {
        return ERR_BUF_TOO_SMALL;
    }
    if let Err(e) = memory.write(caller, out_ptr as usize, bytes) {
        eprintln!("[nx-core] {api_name}: failed to write output: {e}");
        return ERR_INTERNAL;
    }

    bytes.len() as i32
}

fn is_allowed_env_key(key: &str) -> bool {
    if key.is_empty() || key.len() > MAX_ENV_KEY_LEN as usize {
        return false;
    }
    if !key
        .bytes()
        .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_')
    {
        return false;
    }

    key.starts_with("NX_") || key.starts_with("NUMAX_")
}

fn is_valid_event_name(name: &str) -> bool {
    if name.is_empty() || name.len() > MAX_EVENT_NAME_LEN as usize {
        return false;
    }

    name.bytes()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.' | b':'))
}

fn encoded_host_capabilities() -> Vec<u8> {
    HOST_CAPABILITIES.join("\n").into_bytes()
}

fn env_get_impl(
    mut caller: Caller<'_, HostState>,
    key_ptr: u32,
    key_len: u32,
    out_ptr: u32,
    out_cap: u32,
) -> i32 {
    let memory = match get_memory(&mut caller) {
        Some(m) => m,
        None => {
            eprintln!("[nx-core] env_get: no `memory` export on guest");
            return ERR_INTERNAL;
        }
    };

    let key = match read_bytes(&mut caller, &memory, key_ptr, key_len, MAX_ENV_KEY_LEN) {
        Ok(bytes) => bytes,
        Err(e) => {
            eprintln!("[nx-core] env_get: failed to read key: {e}");
            return ERR_INTERNAL;
        }
    };
    let key = match std::str::from_utf8(&key) {
        Ok(key) => key,
        Err(e) => {
            eprintln!("[nx-core] env_get: env key is not UTF-8: {e}");
            return ERR_INTERNAL;
        }
    };

    if !is_allowed_env_key(key) {
        return ERR_NOT_FOUND;
    }

    let value = match env::var(key) {
        Ok(value) => value,
        Err(env::VarError::NotPresent) => return ERR_NOT_FOUND,
        Err(e) => {
            eprintln!("[nx-core] env_get: failed to read env var {key}: {e}");
            return ERR_INTERNAL;
        }
    };

    write_bytes(
        &mut caller,
        &memory,
        out_ptr,
        out_cap,
        value.as_bytes(),
        "env_get",
    )
}

fn host_capabilities_impl(mut caller: Caller<'_, HostState>, out_ptr: u32, out_cap: u32) -> i32 {
    let memory = match get_memory(&mut caller) {
        Some(m) => m,
        None => {
            eprintln!("[nx-core] host_capabilities: no `memory` export on guest");
            return ERR_INTERNAL;
        }
    };
    let capabilities = encoded_host_capabilities();

    write_bytes(
        &mut caller,
        &memory,
        out_ptr,
        out_cap,
        &capabilities,
        "host_capabilities",
    )
}

fn module_id_impl(mut caller: Caller<'_, HostState>, out_ptr: u32, out_cap: u32) -> i32 {
    let memory = match get_memory(&mut caller) {
        Some(m) => m,
        None => {
            eprintln!("[nx-core] module_id: no `memory` export on guest");
            return ERR_INTERNAL;
        }
    };
    let module_id = caller.data().module_id.clone();

    write_bytes(
        &mut caller,
        &memory,
        out_ptr,
        out_cap,
        module_id.as_bytes(),
        "module_id",
    )
}

fn event_emit_impl(
    mut caller: Caller<'_, HostState>,
    name_ptr: u32,
    name_len: u32,
    payload_ptr: u32,
    payload_len: u32,
) -> i32 {
    let memory = match get_memory(&mut caller) {
        Some(m) => m,
        None => {
            eprintln!("[nx-core] event_emit: no `memory` export on guest");
            return ERR_INTERNAL;
        }
    };

    let name = match read_bytes(&mut caller, &memory, name_ptr, name_len, MAX_EVENT_NAME_LEN) {
        Ok(bytes) => bytes,
        Err(e) => {
            eprintln!("[nx-core] event_emit: failed to read event name: {e}");
            return ERR_INTERNAL;
        }
    };
    let name = match std::str::from_utf8(&name) {
        Ok(name) => name,
        Err(e) => {
            eprintln!("[nx-core] event_emit: event name is not UTF-8: {e}");
            return ERR_INTERNAL;
        }
    };
    if !is_valid_event_name(name) {
        eprintln!("[nx-core] event_emit: invalid event name: {name:?}");
        return ERR_INTERNAL;
    }

    let payload = match read_bytes(
        &mut caller,
        &memory,
        payload_ptr,
        payload_len,
        MAX_EVENT_PAYLOAD_LEN,
    ) {
        Ok(bytes) => bytes,
        Err(e) => {
            eprintln!("[nx-core] event_emit: failed to read payload: {e}");
            return ERR_INTERNAL;
        }
    };
    let module_id = caller.data().module_id.clone();

    tracing::info!(
        target: "nx_event",
        module_id = %module_id,
        event = %name,
        payload_len = payload.len(),
        "guest event emitted"
    );

    0
}

fn abort_impl(
    mut caller: Caller<'_, HostState>,
    msg_ptr: u32,
    msg_len: u32,
) -> std::result::Result<(), Error> {
    let memory = match get_memory(&mut caller) {
        Some(m) => m,
        None => return Err(Error::msg("guest abort: no `memory` export on guest")),
    };
    let msg = read_bytes(&mut caller, &memory, msg_ptr, msg_len, MAX_ABORT_MSG_LEN)
        .map(|bytes| String::from_utf8_lossy(&bytes).to_string())
        .unwrap_or_else(|e| format!("failed to read abort message: {e}"));

    Err(Error::msg(format!("guest abort: {msg}")))
}

pub fn add_to_linker(linker: &mut Linker<HostState>) -> Result<()> {
    linker.func_wrap(
        "nx",
        "env_get",
        |caller: Caller<'_, HostState>,
         key_ptr: u32,
         key_len: u32,
         out_ptr: u32,
         out_cap: u32|
         -> i32 { env_get_impl(caller, key_ptr, key_len, out_ptr, out_cap) },
    )?;

    linker.func_wrap(
        "nx",
        "module_id",
        |caller: Caller<'_, HostState>, out_ptr: u32, out_cap: u32| -> i32 {
            module_id_impl(caller, out_ptr, out_cap)
        },
    )?;

    linker.func_wrap(
        "nx",
        "host_capabilities",
        |caller: Caller<'_, HostState>, out_ptr: u32, out_cap: u32| -> i32 {
            host_capabilities_impl(caller, out_ptr, out_cap)
        },
    )?;

    linker.func_wrap(
        "nx",
        "event_emit",
        |caller: Caller<'_, HostState>,
         name_ptr: u32,
         name_len: u32,
         payload_ptr: u32,
         payload_len: u32|
         -> i32 { event_emit_impl(caller, name_ptr, name_len, payload_ptr, payload_len) },
    )?;

    linker.func_wrap(
        "nx",
        "abort",
        |caller: Caller<'_, HostState>,
         msg_ptr: u32,
         msg_len: u32|
         -> std::result::Result<(), Error> { abort_impl(caller, msg_ptr, msg_len) },
    )?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn env_key_policy_allows_only_numax_namespaced_uppercase_keys() {
        assert!(is_allowed_env_key("NX_MODE"));
        assert!(is_allowed_env_key("NUMAX_FEATURE_1"));
        assert!(!is_allowed_env_key("PATH"));
        assert!(!is_allowed_env_key("NX_lower"));
        assert!(!is_allowed_env_key(""));
    }

    #[test]
    fn event_name_policy_accepts_simple_namespaced_names() {
        assert!(is_valid_event_name("module.started"));
        assert!(is_valid_event_name("user:created"));
        assert!(is_valid_event_name("job_1-done"));
        assert!(!is_valid_event_name(""));
        assert!(!is_valid_event_name("bad name"));
        assert!(!is_valid_event_name("bad/slash"));
    }

    #[test]
    fn capabilities_include_phase_12_apis() {
        let capabilities = String::from_utf8(encoded_host_capabilities()).unwrap();

        assert!(capabilities.contains("host_capabilities"));
        assert!(capabilities.contains("event_emit"));
        assert!(capabilities.contains("net_peers"));
        assert!(capabilities.contains("db_scan"));
        assert!(capabilities.contains("db_scan_after"));
        assert!(capabilities.contains("db_keys_after"));
    }
}
