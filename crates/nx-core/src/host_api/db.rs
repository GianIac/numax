use anyhow::Result;
use wasmtime::{Caller, Linker, Memory};

use crate::runtime::HostState;

const ERR_NOT_FOUND: i32 = -1;
const ERR_BUF_TOO_SMALL: i32 = -2;
const ERR_INTERNAL: i32 = -3;

/// Get guest linear memory export (`memory`).
fn get_memory(caller: &mut Caller<'_, HostState>) -> Option<Memory> {
    match caller.get_export("memory") {
        Some(wasmtime::Extern::Memory(mem)) => Some(mem),
        _ => None,
    }
}

/// Read `len` bytes from guest memory at `ptr`.
fn read_bytes(
    caller: &mut Caller<'_, HostState>,
    memory: &Memory,
    ptr: i32,
    len: i32,
) -> Result<Vec<u8>> {
    if ptr < 0 || len < 0 {
        anyhow::bail!("negative ptr/len");
    }

    let mut buf = vec![0u8; len as usize];
    memory.read(caller, ptr as usize, &mut buf)?;
    Ok(buf)
}

pub fn add_to_linker(linker: &mut Linker<HostState>) -> Result<()> {
    
    // nx.db_get(key_ptr, key_len, out_ptr, out_cap) -> i32
    // Returns:
    //   >=0  bytes copied into out_ptr
    //   -1   not found
    //   -2   buffer too small
    //   -3   internal/guest memory error
    linker.func_wrap(
        "nx",
        "db_get",
        |mut caller: Caller<'_, HostState>,
         key_ptr: i32,
         key_len: i32,
         out_ptr: i32,
         out_cap: i32|
         -> i32 {
            let memory = match get_memory(&mut caller) {
                Some(m) => m,
                None => {
                    eprintln!("[nx-core] db_get: no `memory` export on guest");
                    return ERR_INTERNAL;
                }
            };

            if out_ptr < 0 || out_cap < 0 {
                eprintln!("[nx-core] db_get: negative out ptr/cap");
                return ERR_INTERNAL;
            }

            let key = match read_bytes(&mut caller, &memory, key_ptr, key_len) {
                Ok(k) => k,
                Err(e) => {
                    eprintln!("[nx-core] db_get: failed to read key: {e}");
                    return ERR_INTERNAL;
                }
            };

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
        },
    )?;

    // nx.db_set(key_ptr, key_len, val_ptr, val_len) -> i32
    // Returns:
    //   0    ok
    //  -3    internal/guest memory error
    linker.func_wrap(
        "nx",
        "db_set",
        |mut caller: Caller<'_, HostState>,
         key_ptr: i32,
         key_len: i32,
         val_ptr: i32,
         val_len: i32|
         -> i32 {
            let memory = match get_memory(&mut caller) {
                Some(m) => m,
                None => {
                    eprintln!("[nx-core] db_set: no `memory` export on guest");
                    return ERR_INTERNAL;
                }
            };

            let key = match read_bytes(&mut caller, &memory, key_ptr, key_len) {
                Ok(k) => k,
                Err(e) => {
                    eprintln!("[nx-core] db_set: failed to read key: {e}");
                    return ERR_INTERNAL;
                }
            };

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
        },
    )?;

    // nx.db_delete(key_ptr, key_len) -> i32
    // Returns:
    //   0    ok (idempotent)
    //  -3    internal/guest memory error
    linker.func_wrap(
        "nx",
        "db_delete",
        |mut caller: Caller<'_, HostState>, key_ptr: i32, key_len: i32| -> i32 {
            let memory = match get_memory(&mut caller) {
                Some(m) => m,
                None => {
                    eprintln!("[nx-core] db_delete: no `memory` export on guest");
                    return ERR_INTERNAL;
                }
            };

            let key = match read_bytes(&mut caller, &memory, key_ptr, key_len) {
                Ok(k) => k,
                Err(e) => {
                    eprintln!("[nx-core] db_delete: failed to read key: {e}");
                    return ERR_INTERNAL;
                }
            };

            if let Err(e) = caller.data().store.delete(&key) {
                eprintln!("[nx-core] db_delete: store error: {e}");
                return ERR_INTERNAL;
            }

            0
        },
    )?;
    Ok(())
}
