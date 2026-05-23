use std::env;

use anyhow::Result;
use wasmtime::{Caller, Error, Linker, Memory};

use crate::runtime::HostState;

const ERR_NOT_FOUND: i32 = -1;
const ERR_BUF_TOO_SMALL: i32 = -2;
const ERR_INTERNAL: i32 = -3;

const MAX_ENV_KEY_LEN: u32 = 128;
const MAX_ABORT_MSG_LEN: u32 = 8 * 1024;
const MAX_OUT_CAP: u32 = 1024 * 1024;

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
}
