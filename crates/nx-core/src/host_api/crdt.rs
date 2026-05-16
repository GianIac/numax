use anyhow::Result;
use nx_sync::{GCounter, Op};
use wasmtime::{Caller, Linker, Memory};

use crate::runtime::HostState;
use crate::sync_manager::materialize_gcounter_value;

// error codes
const ERR_BUF_TOO_SMALL: i32 = -2;
const ERR_INTERNAL: i32 = -3;
const ERR_RESERVED_KEY: i32 = -4;
const ERR_SYNC_DISABLED: i32 = -5;

// limits
const MAX_KEY_LEN: u32 = 8 * 1024; // 8 KiB, aligned with db.rs
const U64_LEN: u32 = 8;

/// Runtime-reserved key prefix. Any guest-facing host API (db_*, crdt_*)
pub(crate) const RESERVED_PREFIX: &str = "__nx/";

fn get_memory(caller: &mut Caller<'_, HostState>) -> Option<Memory> {
    match caller.get_export("memory") {
        Some(wasmtime::Extern::Memory(mem)) => Some(mem),
        _ => None,
    }
}

/// Read a UTF-8 key from guest memory, validating length and reserved prefix.
fn read_validated_key(
    caller: &mut Caller<'_, HostState>,
    memory: &Memory,
    ptr: u32,
    len: u32,
) -> Result<String, i32> {
    if len > MAX_KEY_LEN {
        eprintln!("[nx-core] crdt: invalid key length: {len} (max {MAX_KEY_LEN})");
        return Err(ERR_INTERNAL);
    }
    let mut buf = vec![0u8; len as usize];
    memory
        .read(&mut *caller, ptr as usize, &mut buf)
        .map_err(|e| {
            eprintln!("[nx-core] crdt: failed to read key: {e}");
            ERR_INTERNAL
        })?;
    let s = std::str::from_utf8(&buf).map_err(|e| {
        eprintln!("[nx-core] crdt: non-UTF8 key: {e}");
        ERR_INTERNAL
    })?;
    if s.starts_with(RESERVED_PREFIX) {
        return Err(ERR_RESERVED_KEY);
    }
    Ok(s.to_string())
}

async fn crdt_gcounter_inc_impl(
    mut caller: Caller<'_, HostState>,
    key_ptr: u32,
    key_len: u32,
    delta: u64,
) -> i32 {
    let memory = match get_memory(&mut caller) {
        Some(m) => m,
        None => {
            eprintln!("[nx-core] crdt_gcounter_inc: no `memory` export on guest");
            return ERR_INTERNAL;
        }
    };

    let key = match read_validated_key(&mut caller, &memory, key_ptr, key_len) {
        Ok(k) => k,
        Err(code) => return code,
    };

    let handle = match caller.data().sync_handle.as_ref() {
        Some(h) => h.clone(),
        None => return ERR_SYNC_DISABLED,
    };

    let op_tx = handle.op_sender();
    let op_permit = match op_tx.try_reserve() {
        Ok(permit) => permit,
        Err(e) => {
            tracing::warn!(error = %e, "crdt_gcounter_inc: broadcast queue full");
            return ERR_INTERNAL;
        }
    };

    // Apply locally and materialize the value before exposing the new state.
    {
        let counters_arc = handle.counters();
        let mut counters = counters_arc.write().await;
        let mut counter = counters.get(&key).cloned().unwrap_or_else(GCounter::new);
        counter.increment(handle.node_id(), delta);
        let total = counter.value();

        if let Err(e) = materialize_gcounter_value(&handle.store(), &key, total) {
            tracing::warn!(error = %e, "crdt_gcounter_inc: failed to materialize counter");
            return ERR_INTERNAL;
        }

        counters.insert(key.clone(), counter);
    }

    let op = Op::gcounter_increment(handle.node_id().clone(), key, delta);
    tracing::debug!(op_id = %op.id, "queued local GCounter increment");
    op_permit.send(op);
    handle.metrics().record_ops(1);

    0
}

async fn crdt_gcounter_value_impl(
    mut caller: Caller<'_, HostState>,
    key_ptr: u32,
    key_len: u32,
    out_ptr: u32,
    out_cap: u32,
) -> i32 {
    let memory = match get_memory(&mut caller) {
        Some(m) => m,
        None => {
            eprintln!("[nx-core] crdt_gcounter_value: no `memory` export on guest");
            return ERR_INTERNAL;
        }
    };

    if out_cap < U64_LEN {
        return ERR_BUF_TOO_SMALL;
    }

    let key = match read_validated_key(&mut caller, &memory, key_ptr, key_len) {
        Ok(k) => k,
        Err(code) => return code,
    };

    let handle = match caller.data().sync_handle.as_ref() {
        Some(h) => h.clone(),
        None => return ERR_SYNC_DISABLED,
    };

    let value: u64 = {
        let counters_arc = handle.counters();
        let counters = counters_arc.read().await;
        counters.get(&key).map(|c| c.value()).unwrap_or(0)
    };

    let bytes = value.to_le_bytes();
    if let Err(e) = memory.write(&mut caller, out_ptr as usize, &bytes) {
        eprintln!("[nx-core] crdt_gcounter_value: failed to write output: {e}");
        return ERR_INTERNAL;
    }

    U64_LEN as i32
}

pub fn add_to_linker(linker: &mut Linker<HostState>) -> Result<()> {
    linker.func_wrap_async(
        "nx",
        "crdt_gcounter_inc",
        |caller: Caller<'_, HostState>, (key_ptr, key_len, delta): (u32, u32, u64)| {
            Box::new(crdt_gcounter_inc_impl(caller, key_ptr, key_len, delta))
        },
    )?;

    linker.func_wrap_async(
        "nx",
        "crdt_gcounter_value",
        |caller: Caller<'_, HostState>,
         (key_ptr, key_len, out_ptr, out_cap): (u32, u32, u32, u32)| {
            Box::new(crdt_gcounter_value_impl(
                caller, key_ptr, key_len, out_ptr, out_cap,
            ))
        },
    )?;

    Ok(())
}
