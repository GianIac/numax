use anyhow::Result;
use wasmtime::{Caller, Linker, Memory};

use crate::runtime::HostState;

const MAX_MSG_LEN: u32 = 8 * 1024; // 8 KiB

/// v2 return codes (only for host_log_v2)
const OK: i32 = 0;
const ERR_INTERNAL: i32 = -3;

fn get_memory(caller: &mut Caller<'_, HostState>) -> Option<Memory> {
    match caller.get_export("memory") {
        Some(wasmtime::Extern::Memory(mem)) => Some(mem),
        _ => None,
    }
}

fn read_bytes(caller: &mut Caller<'_, HostState>,memory: &Memory,ptr: u32,len: u32,) -> Result<Vec<u8>> {
    
    if len > MAX_MSG_LEN {
        anyhow::bail!("message too large: {len} > {MAX_MSG_LEN}");
    }
    let mut buf = vec![0u8; len as usize];
    memory.read(caller, ptr as usize, &mut buf)?;
    Ok(buf)
}

pub fn add_to_linker(linker: &mut Linker<HostState>) -> Result<()> {
    linker.func_wrap(
        "nx",
        "host_log",
        |mut caller: Caller<'_, HostState>, msg_ptr: u32, msg_len: u32| {
            let memory = match get_memory(&mut caller) {
                Some(m) => m,
                None => {
                    eprintln!("[nx-core] host_log(legacy): no `memory` export on guest");
                    return;
                }
            };

            let msg = match read_bytes(&mut caller, &memory, msg_ptr, msg_len) {
                Ok(bytes) => String::from_utf8_lossy(&bytes).to_string(),
                Err(e) => {
                    eprintln!("[nx-core] host_log(legacy): failed to read msg: {e}");
                    return;
                }
            };

            eprintln!("[guest] {msg}");
        },
    )?;


    // Returns: 0  => ok or -3  => internal error (no memory export / invalid read)
    linker.func_wrap(
        "nx",
        "host_log_v2",
        |mut caller: Caller<'_, HostState>, msg_ptr: u32, msg_len: u32| -> i32 {
            let memory = match get_memory(&mut caller) {
                Some(m) => m,
                None => {
                    eprintln!("[nx-core] host_log_v2: no `memory` export on guest");
                    return ERR_INTERNAL;
                }
            };

            let msg = match read_bytes(&mut caller, &memory, msg_ptr, msg_len) {
                Ok(bytes) => String::from_utf8_lossy(&bytes).to_string(),
                Err(e) => {
                    eprintln!("[nx-core] host_log_v2: failed to read msg: {e}");
                    return ERR_INTERNAL;
                }
            };

            eprintln!("[guest] {msg}");
            OK
        },
    )?;

    Ok(())
}
