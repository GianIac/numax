use anyhow::Result;
use wasmtime::{Caller, Linker, Memory};

use crate::runtime::HostState;

const ERR_NOT_FOUND: i32 = -1;
const ERR_BUF_TOO_SMALL: i32 = -2;
const ERR_INTERNAL: i32 = -3;
const ERR_RESERVED_KEY: i32 = -4;

const MAX_KEY_LEN: u32 = 8 * 1024; // 8 KiB
const MAX_VALUE_LEN: u32 = 1024 * 1024; // 1 MiB
const MAX_OUT_CAP: u32 = 1024 * 1024; // 1 MiB

const MAX_READ_LEN: u32 = 1024 * 1024; // 1 MiB

/// Get guest linear memory export as a wasmtime::Memory.
fn get_memory(caller: &mut Caller<'_, HostState>) -> Option<Memory> {
    match caller.get_export("memory") {
        Some(wasmtime::Extern::Memory(mem)) => Some(mem),
        _ => None,
    }
}

/// Read len bytes from guest memory at ptr, returning them as a Vec<u8>.
fn read_bytes(
    caller: &mut Caller<'_, HostState>,
    memory: &Memory,
    ptr: u32,
    len: u32,
) -> Result<Vec<u8>> {
    if len > MAX_READ_LEN {
        anyhow::bail!("requested length too large: {len} > {MAX_READ_LEN}");
    }

    let mut buf = vec![0u8; len as usize];
    memory.read(caller, ptr as usize, &mut buf)?;
    Ok(buf)
}

fn db_get_impl(
    mut caller: Caller<'_, HostState>,
    key_ptr: u32,
    key_len: u32,
    out_ptr: u32,
    out_cap: u32,
) -> i32 {
    let memory = match get_memory(&mut caller) {
        Some(m) => m,
        None => {
            eprintln!("[nx-core] db_get: no `memory` export on guest");
            return ERR_INTERNAL;
        }
    };

    if key_len > MAX_KEY_LEN {
        eprintln!("[nx-core] db_get: invalid key length: {key_len} (max {MAX_KEY_LEN})");
        return ERR_INTERNAL;
    }

    if out_cap > MAX_OUT_CAP {
        eprintln!("[nx-core] db_get: output capacity too large: {out_cap} (max {MAX_OUT_CAP})");
        return ERR_INTERNAL;
    }

    let key = match read_bytes(&mut caller, &memory, key_ptr, key_len) {
        Ok(k) => k,
        Err(e) => {
            eprintln!("[nx-core] db_get: failed to read key: {e}");
            return ERR_INTERNAL;
        }
    };

    if key.starts_with(crate::host_api::crdt::RESERVED_PREFIX.as_bytes()) {
        return ERR_RESERVED_KEY;
    }

    let value = match caller.data().store.get(&key) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("[nx-core] db_get: store error: {e}");
            return ERR_INTERNAL;
        }
    };

    let Some(value) = value else {
        return ERR_NOT_FOUND;
    };

    if value.len() > out_cap as usize {
        return ERR_BUF_TOO_SMALL;
    }

    if let Err(e) = memory.write(&mut caller, out_ptr as usize, &value) {
        eprintln!("[nx-core] db_get: failed to write output: {e}");
        return ERR_INTERNAL;
    }

    value.len() as i32
}

fn db_set_impl(
    mut caller: Caller<'_, HostState>,
    key_ptr: u32,
    key_len: u32,
    val_ptr: u32,
    val_len: u32,
) -> i32 {
    let memory = match get_memory(&mut caller) {
        Some(m) => m,
        None => {
            eprintln!("[nx-core] db_set: no `memory` export on guest");
            return ERR_INTERNAL;
        }
    };

    if key_len > MAX_KEY_LEN {
        eprintln!("[nx-core] db_set: invalid key length: {key_len} (max {MAX_KEY_LEN})");
        return ERR_INTERNAL;
    }
    if val_len > MAX_VALUE_LEN {
        eprintln!("[nx-core] db_set: invalid value length: {val_len} (max {MAX_VALUE_LEN})");
        return ERR_INTERNAL;
    }

    let key = match read_bytes(&mut caller, &memory, key_ptr, key_len) {
        Ok(k) => k,
        Err(e) => {
            eprintln!("[nx-core] db_set: failed to read key: {e}");
            return ERR_INTERNAL;
        }
    };

    if key.starts_with(crate::host_api::crdt::RESERVED_PREFIX.as_bytes()) {
        return ERR_RESERVED_KEY;
    }

    let val = match read_bytes(&mut caller, &memory, val_ptr, val_len) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("[nx-core] db_set: failed to read value: {e}");
            return ERR_INTERNAL;
        }
    };

    if let Err(e) = caller.data().store.set(&key, &val) {
        eprintln!("[nx-core] db_set: store error: {e}");
        return ERR_INTERNAL;
    }

    0
}

fn db_delete_impl(mut caller: Caller<'_, HostState>, key_ptr: u32, key_len: u32) -> i32 {
    let memory = match get_memory(&mut caller) {
        Some(m) => m,
        None => {
            eprintln!("[nx-core] db_delete: no `memory` export on guest");
            return ERR_INTERNAL;
        }
    };

    if key_len > MAX_KEY_LEN {
        eprintln!("[nx-core] db_delete: invalid key length: {key_len} (max {MAX_KEY_LEN})");
        return ERR_INTERNAL;
    }

    let key = match read_bytes(&mut caller, &memory, key_ptr, key_len) {
        Ok(k) => k,
        Err(e) => {
            eprintln!("[nx-core] db_delete: failed to read key: {e}");
            return ERR_INTERNAL;
        }
    };

    if key.starts_with(crate::host_api::crdt::RESERVED_PREFIX.as_bytes()) {
        return ERR_RESERVED_KEY;
    }

    if let Err(e) = caller.data().store.delete(&key) {
        eprintln!("[nx-core] db_delete: store error: {e}");
        return ERR_INTERNAL;
    }

    0
}

pub fn add_to_linker(linker: &mut Linker<HostState>) -> Result<()> {
    // ABI CANONICA: solo db_*
    linker.func_wrap(
        "nx",
        "db_get",
        |caller: Caller<'_, HostState>,
         key_ptr: u32,
         key_len: u32,
         out_ptr: u32,
         out_cap: u32|
         -> i32 { db_get_impl(caller, key_ptr, key_len, out_ptr, out_cap) },
    )?;

    linker.func_wrap(
        "nx",
        "db_set",
        |caller: Caller<'_, HostState>,
         key_ptr: u32,
         key_len: u32,
         val_ptr: u32,
         val_len: u32|
         -> i32 { db_set_impl(caller, key_ptr, key_len, val_ptr, val_len) },
    )?;

    linker.func_wrap(
        "nx",
        "db_delete",
        |caller: Caller<'_, HostState>, key_ptr: u32, key_len: u32| -> i32 {
            db_delete_impl(caller, key_ptr, key_len)
        },
    )?;

    Ok(())
}
