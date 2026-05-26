use anyhow::Result;
use nx_sync::{GCounter, LwwMap, LwwRegister, ORSet, Op, OpKind, PNCounter, Rga};
use std::time::{SystemTime, UNIX_EPOCH};
use wasmtime::{Caller, Linker, Memory};

use crate::runtime::HostState;
use crate::sync_manager::{
    persist_gcounter_state, persist_lww_map_state, persist_lww_register_state, persist_orset_state,
    persist_pncounter_state, persist_rga_state,
};

// error codes
const ERR_NOT_FOUND: i32 = -1;
const ERR_BUF_TOO_SMALL: i32 = -2;
const ERR_INTERNAL: i32 = -3;
const ERR_RESERVED_KEY: i32 = -4;
const ERR_SYNC_DISABLED: i32 = -5;

// limits
const MAX_KEY_LEN: u32 = 8 * 1024; // 8 KiB, aligned with db.rs
const MAX_ELEMENT_LEN: u32 = 8 * 1024;
const MAX_FIELD_LEN: u32 = 8 * 1024;
const MAX_RGA_ID_LEN: u32 = 8 * 1024;
const MAX_VALUE_LEN: u32 = 1024 * 1024; // 1 MiB, aligned with db.rs
const MAX_OUT_CAP: u32 = 1024 * 1024; // 1 MiB
const U64_LEN: u32 = 8;
const I64_LEN: u32 = 8;

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

fn read_value_bytes(
    caller: &mut Caller<'_, HostState>,
    memory: &Memory,
    ptr: u32,
    len: u32,
    api_name: &str,
) -> Result<Vec<u8>, i32> {
    if len > MAX_VALUE_LEN {
        eprintln!("[nx-core] {api_name}: invalid value length: {len} (max {MAX_VALUE_LEN})");
        return Err(ERR_INTERNAL);
    }

    let mut buf = vec![0u8; len as usize];
    memory.read(caller, ptr as usize, &mut buf).map_err(|e| {
        eprintln!("[nx-core] {api_name}: failed to read value: {e}");
        ERR_INTERNAL
    })?;
    Ok(buf)
}

fn read_utf8_arg(
    caller: &mut Caller<'_, HostState>,
    memory: &Memory,
    ptr: u32,
    len: u32,
    max_len: u32,
    api_name: &str,
    arg_name: &str,
) -> Result<String, i32> {
    if len > max_len {
        eprintln!("[nx-core] {api_name}: invalid {arg_name} length: {len} (max {max_len})");
        return Err(ERR_INTERNAL);
    }

    let mut buf = vec![0u8; len as usize];
    memory.read(caller, ptr as usize, &mut buf).map_err(|e| {
        eprintln!("[nx-core] {api_name}: failed to read {arg_name}: {e}");
        ERR_INTERNAL
    })?;
    String::from_utf8(buf).map_err(|e| {
        eprintln!("[nx-core] {api_name}: {arg_name} is not UTF-8: {e}");
        ERR_INTERNAL
    })
}

fn encode_string_list(values: &[String]) -> Result<Vec<u8>, i32> {
    let mut encoded = Vec::new();
    let count = u32::try_from(values.len()).map_err(|_| ERR_INTERNAL)?;
    encoded.extend_from_slice(&count.to_le_bytes());

    for value in values {
        let bytes = value.as_bytes();
        let len = u32::try_from(bytes.len()).map_err(|_| ERR_INTERNAL)?;
        encoded.extend_from_slice(&len.to_le_bytes());
        encoded.extend_from_slice(bytes);
    }

    Ok(encoded)
}

fn encode_bytes_list(values: &[Vec<u8>]) -> Result<Vec<u8>, i32> {
    let mut encoded = Vec::new();
    let count = u32::try_from(values.len()).map_err(|_| ERR_INTERNAL)?;
    encoded.extend_from_slice(&count.to_le_bytes());

    for value in values {
        let len = u32::try_from(value.len()).map_err(|_| ERR_INTERNAL)?;
        encoded.extend_from_slice(&len.to_le_bytes());
        encoded.extend_from_slice(value);
    }

    Ok(encoded)
}

fn encode_lww_map_entries(entries: &[(String, Vec<u8>)]) -> Result<Vec<u8>, i32> {
    let mut encoded = Vec::new();
    let count = u32::try_from(entries.len()).map_err(|_| ERR_INTERNAL)?;
    encoded.extend_from_slice(&count.to_le_bytes());

    for (field, value) in entries {
        let field_bytes = field.as_bytes();
        let field_len = u32::try_from(field_bytes.len()).map_err(|_| ERR_INTERNAL)?;
        let value_len = u32::try_from(value.len()).map_err(|_| ERR_INTERNAL)?;
        encoded.extend_from_slice(&field_len.to_le_bytes());
        encoded.extend_from_slice(field_bytes);
        encoded.extend_from_slice(&value_len.to_le_bytes());
        encoded.extend_from_slice(value);
    }

    Ok(encoded)
}

fn unix_epoch_millis() -> Result<u64, i32> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| {
            eprintln!("[nx-core] crdt_lww_set: system clock before Unix epoch: {e}");
            ERR_INTERNAL
        })?
        .as_millis();
    u64::try_from(millis).map_err(|_| {
        eprintln!("[nx-core] crdt_lww_set: Unix timestamp does not fit into u64");
        ERR_INTERNAL
    })
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
            handle.metrics().record_sync_error();
            tracing::warn!(error = %e, "crdt_gcounter_inc: broadcast queue full");
            return ERR_INTERNAL;
        }
    };

    // Apply locally and persist the CRDT state before exposing the new value.
    {
        let counters_arc = handle.counters();
        let mut counters = counters_arc.write().await;
        let mut counter = counters.get(&key).cloned().unwrap_or_else(GCounter::new);
        counter.increment(handle.node_id(), delta);
        if let Err(e) = persist_gcounter_state(&handle.store(), &key, &counter) {
            handle.metrics().record_sync_error();
            tracing::warn!(error = %e, "crdt_gcounter_inc: failed to persist counter");
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

async fn crdt_pncounter_inc_impl(
    mut caller: Caller<'_, HostState>,
    key_ptr: u32,
    key_len: u32,
    delta: u64,
) -> i32 {
    crdt_pncounter_change_impl(
        &mut caller,
        key_ptr,
        key_len,
        delta,
        PNCounterChange::Increment,
    )
    .await
}

async fn crdt_pncounter_dec_impl(
    mut caller: Caller<'_, HostState>,
    key_ptr: u32,
    key_len: u32,
    delta: u64,
) -> i32 {
    crdt_pncounter_change_impl(
        &mut caller,
        key_ptr,
        key_len,
        delta,
        PNCounterChange::Decrement,
    )
    .await
}

#[derive(Debug, Clone, Copy)]
enum PNCounterChange {
    Increment,
    Decrement,
}

async fn crdt_pncounter_change_impl(
    caller: &mut Caller<'_, HostState>,
    key_ptr: u32,
    key_len: u32,
    delta: u64,
    change: PNCounterChange,
) -> i32 {
    let api_name = match change {
        PNCounterChange::Increment => "crdt_pncounter_inc",
        PNCounterChange::Decrement => "crdt_pncounter_dec",
    };

    let memory = match get_memory(caller) {
        Some(m) => m,
        None => {
            eprintln!("[nx-core] {api_name}: no `memory` export on guest");
            return ERR_INTERNAL;
        }
    };

    let key = match read_validated_key(caller, &memory, key_ptr, key_len) {
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
            handle.metrics().record_sync_error();
            tracing::warn!(error = %e, "{api_name}: broadcast queue full");
            return ERR_INTERNAL;
        }
    };

    {
        let counters_arc = handle.pncounters();
        let mut counters = counters_arc.write().await;
        let mut counter = counters.get(&key).cloned().unwrap_or_else(PNCounter::new);
        match change {
            PNCounterChange::Increment => counter.increment(handle.node_id(), delta),
            PNCounterChange::Decrement => counter.decrement(handle.node_id(), delta),
        }

        if let Err(e) = persist_pncounter_state(&handle.store(), &key, &counter) {
            handle.metrics().record_sync_error();
            tracing::warn!(error = %e, "{api_name}: failed to persist counter");
            return ERR_INTERNAL;
        }

        counters.insert(key.clone(), counter);
    }

    let op = match change {
        PNCounterChange::Increment => Op::pncounter_increment(handle.node_id().clone(), key, delta),
        PNCounterChange::Decrement => Op::pncounter_decrement(handle.node_id().clone(), key, delta),
    };
    tracing::debug!(op_id = %op.id, api = api_name, "queued local PNCounter op");
    op_permit.send(op);
    handle.metrics().record_ops(1);

    0
}

async fn crdt_pncounter_value_impl(
    mut caller: Caller<'_, HostState>,
    key_ptr: u32,
    key_len: u32,
    out_ptr: u32,
    out_cap: u32,
) -> i32 {
    let memory = match get_memory(&mut caller) {
        Some(m) => m,
        None => {
            eprintln!("[nx-core] crdt_pncounter_value: no `memory` export on guest");
            return ERR_INTERNAL;
        }
    };

    if out_cap < I64_LEN {
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

    let value: i64 = {
        let counters_arc = handle.pncounters();
        let counters = counters_arc.read().await;
        counters.get(&key).map(|c| c.value()).unwrap_or(0)
    };

    let bytes = value.to_le_bytes();
    if let Err(e) = memory.write(&mut caller, out_ptr as usize, &bytes) {
        eprintln!("[nx-core] crdt_pncounter_value: failed to write output: {e}");
        return ERR_INTERNAL;
    }

    I64_LEN as i32
}

async fn crdt_lww_set_impl(
    mut caller: Caller<'_, HostState>,
    key_ptr: u32,
    key_len: u32,
    value_ptr: u32,
    value_len: u32,
) -> i32 {
    let memory = match get_memory(&mut caller) {
        Some(m) => m,
        None => {
            eprintln!("[nx-core] crdt_lww_set: no `memory` export on guest");
            return ERR_INTERNAL;
        }
    };

    let key = match read_validated_key(&mut caller, &memory, key_ptr, key_len) {
        Ok(k) => k,
        Err(code) => return code,
    };
    let value = match read_value_bytes(&mut caller, &memory, value_ptr, value_len, "crdt_lww_set") {
        Ok(value) => value,
        Err(code) => return code,
    };
    let observed_timestamp_ms = match unix_epoch_millis() {
        Ok(timestamp_ms) => timestamp_ms,
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
            handle.metrics().record_sync_error();
            tracing::warn!(error = %e, "crdt_lww_set: broadcast queue full");
            return ERR_INTERNAL;
        }
    };

    let mut timestamp_ms = observed_timestamp_ms;
    {
        let registers_arc = handle.lww_registers();
        let mut registers = registers_arc.write().await;
        if let Some(existing) = registers.get(&key) {
            timestamp_ms = timestamp_ms.max(existing.timestamp_ms().saturating_add(1));
        }
        let candidate = LwwRegister::new(value.clone(), timestamp_ms, handle.node_id().clone());
        let next_register = match registers.get(&key) {
            Some(register) => {
                let mut next = register.clone();
                if next.merge(&candidate) {
                    Some(next)
                } else {
                    None
                }
            }
            None => Some(candidate.clone()),
        };

        if let Some(register) = next_register {
            if let Err(e) = persist_lww_register_state(&handle.store(), &key, &register) {
                handle.metrics().record_sync_error();
                tracing::warn!(error = %e, "crdt_lww_set: failed to persist register");
                return ERR_INTERNAL;
            }
            registers.insert(key.clone(), register);
        }
    }

    let op = Op::lww_register_set(handle.node_id().clone(), key, value, timestamp_ms);
    tracing::debug!(op_id = %op.id, "queued local LWW-Register set");
    op_permit.send(op);
    handle.metrics().record_ops(1);

    0
}

async fn crdt_lww_get_impl(
    mut caller: Caller<'_, HostState>,
    key_ptr: u32,
    key_len: u32,
    out_ptr: u32,
    out_cap: u32,
) -> i32 {
    let memory = match get_memory(&mut caller) {
        Some(m) => m,
        None => {
            eprintln!("[nx-core] crdt_lww_get: no `memory` export on guest");
            return ERR_INTERNAL;
        }
    };

    if out_cap > MAX_OUT_CAP {
        eprintln!(
            "[nx-core] crdt_lww_get: output capacity too large: {out_cap} (max {MAX_OUT_CAP})"
        );
        return ERR_INTERNAL;
    }

    let key = match read_validated_key(&mut caller, &memory, key_ptr, key_len) {
        Ok(k) => k,
        Err(code) => return code,
    };

    let handle = match caller.data().sync_handle.as_ref() {
        Some(h) => h.clone(),
        None => return ERR_SYNC_DISABLED,
    };

    let value = {
        let registers_arc = handle.lww_registers();
        let registers = registers_arc.read().await;
        let Some(register) = registers.get(&key) else {
            return ERR_NOT_FOUND;
        };
        register.value_bytes()
    };

    if value.len() > out_cap as usize {
        return ERR_BUF_TOO_SMALL;
    }

    if let Err(e) = memory.write(&mut caller, out_ptr as usize, &value) {
        eprintln!("[nx-core] crdt_lww_get: failed to write output: {e}");
        return ERR_INTERNAL;
    }

    value.len() as i32
}

async fn crdt_lww_map_set_impl(
    mut caller: Caller<'_, HostState>,
    key_ptr: u32,
    key_len: u32,
    field_ptr: u32,
    field_len: u32,
    value_ptr: u32,
    value_len: u32,
) -> i32 {
    let memory = match get_memory(&mut caller) {
        Some(m) => m,
        None => {
            eprintln!("[nx-core] crdt_lww_map_set: no `memory` export on guest");
            return ERR_INTERNAL;
        }
    };

    let key = match read_validated_key(&mut caller, &memory, key_ptr, key_len) {
        Ok(k) => k,
        Err(code) => return code,
    };
    let field = match read_utf8_arg(
        &mut caller,
        &memory,
        field_ptr,
        field_len,
        MAX_FIELD_LEN,
        "crdt_lww_map_set",
        "field",
    ) {
        Ok(field) => field,
        Err(code) => return code,
    };
    let value = match read_value_bytes(
        &mut caller,
        &memory,
        value_ptr,
        value_len,
        "crdt_lww_map_set",
    ) {
        Ok(value) => value,
        Err(code) => return code,
    };
    let observed_timestamp_ms = match unix_epoch_millis() {
        Ok(timestamp_ms) => timestamp_ms,
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
            handle.metrics().record_sync_error();
            tracing::warn!(error = %e, "crdt_lww_map_set: broadcast queue full");
            return ERR_INTERNAL;
        }
    };

    let mut timestamp_ms = observed_timestamp_ms;
    {
        let maps_arc = handle.lww_maps();
        let mut maps = maps_arc.write().await;
        let mut map = maps.get(&key).cloned().unwrap_or_else(LwwMap::new);
        if let Some(existing) = map.entry(&field) {
            timestamp_ms = timestamp_ms.max(existing.timestamp_ms().saturating_add(1));
        }
        map.set(
            field.clone(),
            value.clone(),
            timestamp_ms,
            handle.node_id().clone(),
        );
        if let Err(e) = persist_lww_map_state(&handle.store(), &key, &map) {
            handle.metrics().record_sync_error();
            tracing::warn!(error = %e, "crdt_lww_map_set: failed to persist map");
            return ERR_INTERNAL;
        }
        maps.insert(key.clone(), map);
    }

    let op = Op::lww_map_set(handle.node_id().clone(), key, field, value, timestamp_ms);
    tracing::debug!(op_id = %op.id, "queued local LWW-Map set");
    op_permit.send(op);
    handle.metrics().record_ops(1);

    0
}

async fn crdt_lww_map_remove_impl(
    mut caller: Caller<'_, HostState>,
    key_ptr: u32,
    key_len: u32,
    field_ptr: u32,
    field_len: u32,
) -> i32 {
    let memory = match get_memory(&mut caller) {
        Some(m) => m,
        None => {
            eprintln!("[nx-core] crdt_lww_map_remove: no `memory` export on guest");
            return ERR_INTERNAL;
        }
    };

    let key = match read_validated_key(&mut caller, &memory, key_ptr, key_len) {
        Ok(k) => k,
        Err(code) => return code,
    };
    let field = match read_utf8_arg(
        &mut caller,
        &memory,
        field_ptr,
        field_len,
        MAX_FIELD_LEN,
        "crdt_lww_map_remove",
        "field",
    ) {
        Ok(field) => field,
        Err(code) => return code,
    };
    let observed_timestamp_ms = match unix_epoch_millis() {
        Ok(timestamp_ms) => timestamp_ms,
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
            handle.metrics().record_sync_error();
            tracing::warn!(error = %e, "crdt_lww_map_remove: broadcast queue full");
            return ERR_INTERNAL;
        }
    };

    let mut timestamp_ms = observed_timestamp_ms;
    {
        let maps_arc = handle.lww_maps();
        let mut maps = maps_arc.write().await;
        let mut map = maps.get(&key).cloned().unwrap_or_else(LwwMap::new);
        if let Some(existing) = map.entry(&field) {
            timestamp_ms = timestamp_ms.max(existing.timestamp_ms().saturating_add(1));
        }
        map.remove(field.clone(), timestamp_ms, handle.node_id().clone());
        if let Err(e) = persist_lww_map_state(&handle.store(), &key, &map) {
            handle.metrics().record_sync_error();
            tracing::warn!(error = %e, "crdt_lww_map_remove: failed to persist map");
            return ERR_INTERNAL;
        }
        maps.insert(key.clone(), map);
    }

    let op = Op::lww_map_remove(handle.node_id().clone(), key, field, timestamp_ms);
    tracing::debug!(op_id = %op.id, "queued local LWW-Map remove");
    op_permit.send(op);
    handle.metrics().record_ops(1);

    0
}

async fn crdt_lww_map_get_impl(
    mut caller: Caller<'_, HostState>,
    key_ptr: u32,
    key_len: u32,
    field_ptr: u32,
    field_len: u32,
    out_ptr: u32,
    out_cap: u32,
) -> i32 {
    let memory = match get_memory(&mut caller) {
        Some(m) => m,
        None => {
            eprintln!("[nx-core] crdt_lww_map_get: no `memory` export on guest");
            return ERR_INTERNAL;
        }
    };

    if out_cap > MAX_OUT_CAP {
        eprintln!(
            "[nx-core] crdt_lww_map_get: output capacity too large: {out_cap} (max {MAX_OUT_CAP})"
        );
        return ERR_INTERNAL;
    }

    let key = match read_validated_key(&mut caller, &memory, key_ptr, key_len) {
        Ok(k) => k,
        Err(code) => return code,
    };
    let field = match read_utf8_arg(
        &mut caller,
        &memory,
        field_ptr,
        field_len,
        MAX_FIELD_LEN,
        "crdt_lww_map_get",
        "field",
    ) {
        Ok(field) => field,
        Err(code) => return code,
    };

    let handle = match caller.data().sync_handle.as_ref() {
        Some(h) => h.clone(),
        None => return ERR_SYNC_DISABLED,
    };

    let value = {
        let maps_arc = handle.lww_maps();
        let maps = maps_arc.read().await;
        let Some(map) = maps.get(&key) else {
            return ERR_NOT_FOUND;
        };
        let Some(value) = map.get_bytes(&field) else {
            return ERR_NOT_FOUND;
        };
        value
    };

    if value.len() > out_cap as usize {
        return ERR_BUF_TOO_SMALL;
    }

    if let Err(e) = memory.write(&mut caller, out_ptr as usize, &value) {
        eprintln!("[nx-core] crdt_lww_map_get: failed to write output: {e}");
        return ERR_INTERNAL;
    }

    value.len() as i32
}

async fn crdt_lww_map_contains_impl(
    mut caller: Caller<'_, HostState>,
    key_ptr: u32,
    key_len: u32,
    field_ptr: u32,
    field_len: u32,
) -> i32 {
    let memory = match get_memory(&mut caller) {
        Some(m) => m,
        None => {
            eprintln!("[nx-core] crdt_lww_map_contains: no `memory` export on guest");
            return ERR_INTERNAL;
        }
    };

    let key = match read_validated_key(&mut caller, &memory, key_ptr, key_len) {
        Ok(k) => k,
        Err(code) => return code,
    };
    let field = match read_utf8_arg(
        &mut caller,
        &memory,
        field_ptr,
        field_len,
        MAX_FIELD_LEN,
        "crdt_lww_map_contains",
        "field",
    ) {
        Ok(field) => field,
        Err(code) => return code,
    };

    let handle = match caller.data().sync_handle.as_ref() {
        Some(h) => h.clone(),
        None => return ERR_SYNC_DISABLED,
    };

    let contains = {
        let maps_arc = handle.lww_maps();
        let maps = maps_arc.read().await;
        maps.get(&key)
            .map(|map| map.contains(&field))
            .unwrap_or(false)
    };

    i32::from(contains)
}

async fn crdt_lww_map_entries_impl(
    mut caller: Caller<'_, HostState>,
    key_ptr: u32,
    key_len: u32,
    out_ptr: u32,
    out_cap: u32,
) -> i32 {
    let memory = match get_memory(&mut caller) {
        Some(m) => m,
        None => {
            eprintln!("[nx-core] crdt_lww_map_entries: no `memory` export on guest");
            return ERR_INTERNAL;
        }
    };

    if out_cap > MAX_OUT_CAP {
        eprintln!(
            "[nx-core] crdt_lww_map_entries: output capacity too large: {out_cap} (max {MAX_OUT_CAP})"
        );
        return ERR_INTERNAL;
    }

    let key = match read_validated_key(&mut caller, &memory, key_ptr, key_len) {
        Ok(k) => k,
        Err(code) => return code,
    };

    let handle = match caller.data().sync_handle.as_ref() {
        Some(h) => h.clone(),
        None => return ERR_SYNC_DISABLED,
    };

    let entries = {
        let maps_arc = handle.lww_maps();
        let maps = maps_arc.read().await;
        maps.get(&key).map(|map| map.entries()).unwrap_or_default()
    };
    let encoded = match encode_lww_map_entries(&entries) {
        Ok(encoded) => encoded,
        Err(code) => return code,
    };

    if encoded.len() > out_cap as usize {
        return ERR_BUF_TOO_SMALL;
    }

    if let Err(e) = memory.write(&mut caller, out_ptr as usize, &encoded) {
        eprintln!("[nx-core] crdt_lww_map_entries: failed to write output: {e}");
        return ERR_INTERNAL;
    }

    encoded.len() as i32
}

async fn crdt_orset_add_impl(
    mut caller: Caller<'_, HostState>,
    key_ptr: u32,
    key_len: u32,
    element_ptr: u32,
    element_len: u32,
) -> i32 {
    let memory = match get_memory(&mut caller) {
        Some(m) => m,
        None => {
            eprintln!("[nx-core] crdt_orset_add: no `memory` export on guest");
            return ERR_INTERNAL;
        }
    };

    let key = match read_validated_key(&mut caller, &memory, key_ptr, key_len) {
        Ok(k) => k,
        Err(code) => return code,
    };
    let element = match read_utf8_arg(
        &mut caller,
        &memory,
        element_ptr,
        element_len,
        MAX_ELEMENT_LEN,
        "crdt_orset_add",
        "element",
    ) {
        Ok(element) => element,
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
            handle.metrics().record_sync_error();
            tracing::warn!(error = %e, "crdt_orset_add: broadcast queue full");
            return ERR_INTERNAL;
        }
    };

    let op = Op::orset_add_with_op_id_tag(handle.node_id().clone(), key.clone(), element.clone());
    let tag = match &op.kind {
        OpKind::ORSetAdd { tag, .. } => tag.clone(),
        _ => {
            handle.metrics().record_sync_error();
            return ERR_INTERNAL;
        }
    };

    {
        let sets_arc = handle.orsets();
        let mut sets = sets_arc.write().await;
        let mut set = sets.get(&key).cloned().unwrap_or_else(ORSet::new);
        set.add(element, tag);
        if let Err(e) = persist_orset_state(&handle.store(), &key, &set) {
            handle.metrics().record_sync_error();
            tracing::warn!(error = %e, "crdt_orset_add: failed to persist set");
            return ERR_INTERNAL;
        }
        sets.insert(key, set);
    }

    tracing::debug!(op_id = %op.id, "queued local ORSet add");
    op_permit.send(op);
    handle.metrics().record_ops(1);

    0
}

async fn crdt_orset_remove_impl(
    mut caller: Caller<'_, HostState>,
    key_ptr: u32,
    key_len: u32,
    element_ptr: u32,
    element_len: u32,
) -> i32 {
    let memory = match get_memory(&mut caller) {
        Some(m) => m,
        None => {
            eprintln!("[nx-core] crdt_orset_remove: no `memory` export on guest");
            return ERR_INTERNAL;
        }
    };

    let key = match read_validated_key(&mut caller, &memory, key_ptr, key_len) {
        Ok(k) => k,
        Err(code) => return code,
    };
    let element = match read_utf8_arg(
        &mut caller,
        &memory,
        element_ptr,
        element_len,
        MAX_ELEMENT_LEN,
        "crdt_orset_remove",
        "element",
    ) {
        Ok(element) => element,
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
            handle.metrics().record_sync_error();
            tracing::warn!(error = %e, "crdt_orset_remove: broadcast queue full");
            return ERR_INTERNAL;
        }
    };

    let observed_tags = {
        let sets_arc = handle.orsets();
        let mut sets = sets_arc.write().await;
        let mut set = sets.get(&key).cloned().unwrap_or_else(ORSet::new);
        let observed_tags = set.remove(&element);
        if observed_tags.is_empty() {
            return 0;
        }
        if let Err(e) = persist_orset_state(&handle.store(), &key, &set) {
            handle.metrics().record_sync_error();
            tracing::warn!(error = %e, "crdt_orset_remove: failed to persist set");
            return ERR_INTERNAL;
        }
        sets.insert(key.clone(), set);
        observed_tags
    };

    let op = Op::orset_remove(handle.node_id().clone(), key, element, observed_tags);
    tracing::debug!(op_id = %op.id, "queued local ORSet remove");
    op_permit.send(op);
    handle.metrics().record_ops(1);

    0
}

async fn crdt_orset_contains_impl(
    mut caller: Caller<'_, HostState>,
    key_ptr: u32,
    key_len: u32,
    element_ptr: u32,
    element_len: u32,
) -> i32 {
    let memory = match get_memory(&mut caller) {
        Some(m) => m,
        None => {
            eprintln!("[nx-core] crdt_orset_contains: no `memory` export on guest");
            return ERR_INTERNAL;
        }
    };

    let key = match read_validated_key(&mut caller, &memory, key_ptr, key_len) {
        Ok(k) => k,
        Err(code) => return code,
    };
    let element = match read_utf8_arg(
        &mut caller,
        &memory,
        element_ptr,
        element_len,
        MAX_ELEMENT_LEN,
        "crdt_orset_contains",
        "element",
    ) {
        Ok(element) => element,
        Err(code) => return code,
    };

    let handle = match caller.data().sync_handle.as_ref() {
        Some(h) => h.clone(),
        None => return ERR_SYNC_DISABLED,
    };

    let contains = {
        let sets_arc = handle.orsets();
        let sets = sets_arc.read().await;
        sets.get(&key)
            .map(|set| set.contains(&element))
            .unwrap_or(false)
    };

    i32::from(contains)
}

async fn crdt_orset_elements_impl(
    mut caller: Caller<'_, HostState>,
    key_ptr: u32,
    key_len: u32,
    out_ptr: u32,
    out_cap: u32,
) -> i32 {
    let memory = match get_memory(&mut caller) {
        Some(m) => m,
        None => {
            eprintln!("[nx-core] crdt_orset_elements: no `memory` export on guest");
            return ERR_INTERNAL;
        }
    };

    if out_cap > MAX_OUT_CAP {
        eprintln!(
            "[nx-core] crdt_orset_elements: output capacity too large: {out_cap} (max {MAX_OUT_CAP})"
        );
        return ERR_INTERNAL;
    }

    let key = match read_validated_key(&mut caller, &memory, key_ptr, key_len) {
        Ok(k) => k,
        Err(code) => return code,
    };

    let handle = match caller.data().sync_handle.as_ref() {
        Some(h) => h.clone(),
        None => return ERR_SYNC_DISABLED,
    };

    let elements = {
        let sets_arc = handle.orsets();
        let sets = sets_arc.read().await;
        sets.get(&key).map(|set| set.elements()).unwrap_or_default()
    };
    let encoded = match encode_string_list(&elements) {
        Ok(encoded) => encoded,
        Err(code) => return code,
    };

    if encoded.len() > out_cap as usize {
        return ERR_BUF_TOO_SMALL;
    }

    if let Err(e) = memory.write(&mut caller, out_ptr as usize, &encoded) {
        eprintln!("[nx-core] crdt_orset_elements: failed to write output: {e}");
        return ERR_INTERNAL;
    }

    encoded.len() as i32
}

#[derive(Debug, Clone, Copy)]
struct RgaInsertArgs {
    key_ptr: u32,
    key_len: u32,
    parent_ptr: u32,
    parent_len: u32,
    value_ptr: u32,
    value_len: u32,
    out_id_ptr: u32,
    out_id_cap: u32,
}

async fn crdt_rga_insert_impl(mut caller: Caller<'_, HostState>, args: RgaInsertArgs) -> i32 {
    let memory = match get_memory(&mut caller) {
        Some(m) => m,
        None => {
            eprintln!("[nx-core] crdt_rga_insert: no `memory` export on guest");
            return ERR_INTERNAL;
        }
    };

    if args.out_id_cap > MAX_OUT_CAP {
        eprintln!(
            "[nx-core] crdt_rga_insert: output capacity too large: {} (max {MAX_OUT_CAP})",
            args.out_id_cap
        );
        return ERR_INTERNAL;
    }

    let key = match read_validated_key(&mut caller, &memory, args.key_ptr, args.key_len) {
        Ok(k) => k,
        Err(code) => return code,
    };
    let parent = if args.parent_len == 0 {
        None
    } else {
        match read_utf8_arg(
            &mut caller,
            &memory,
            args.parent_ptr,
            args.parent_len,
            MAX_RGA_ID_LEN,
            "crdt_rga_insert",
            "parent",
        ) {
            Ok(parent) => Some(parent),
            Err(code) => return code,
        }
    };
    let value = match read_value_bytes(
        &mut caller,
        &memory,
        args.value_ptr,
        args.value_len,
        "crdt_rga_insert",
    ) {
        Ok(value) => value,
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
            handle.metrics().record_sync_error();
            tracing::warn!(error = %e, "crdt_rga_insert: broadcast queue full");
            return ERR_INTERNAL;
        }
    };

    let op = Op::rga_insert_with_op_id(
        handle.node_id().clone(),
        key.clone(),
        parent.clone(),
        value.clone(),
    );
    let element_id = match &op.kind {
        OpKind::RgaInsert { id, .. } => id.clone(),
        _ => {
            handle.metrics().record_sync_error();
            return ERR_INTERNAL;
        }
    };
    let id_bytes = element_id.as_bytes();
    if id_bytes.len() > args.out_id_cap as usize {
        return ERR_BUF_TOO_SMALL;
    }

    {
        let rgas_arc = handle.rgas();
        let mut rgas = rgas_arc.write().await;
        let mut rga = rgas.get(&key).cloned().unwrap_or_else(Rga::new);
        rga.insert(element_id.clone(), parent, value);
        if let Err(e) = persist_rga_state(&handle.store(), &key, &rga) {
            handle.metrics().record_sync_error();
            tracing::warn!(error = %e, "crdt_rga_insert: failed to persist sequence");
            return ERR_INTERNAL;
        }
        rgas.insert(key, rga);
    }

    if let Err(e) = memory.write(&mut caller, args.out_id_ptr as usize, id_bytes) {
        eprintln!("[nx-core] crdt_rga_insert: failed to write output id: {e}");
        return ERR_INTERNAL;
    }

    tracing::debug!(op_id = %op.id, "queued local RGA insert");
    op_permit.send(op);
    handle.metrics().record_ops(1);

    id_bytes.len() as i32
}

async fn crdt_rga_delete_impl(
    mut caller: Caller<'_, HostState>,
    key_ptr: u32,
    key_len: u32,
    id_ptr: u32,
    id_len: u32,
) -> i32 {
    let memory = match get_memory(&mut caller) {
        Some(m) => m,
        None => {
            eprintln!("[nx-core] crdt_rga_delete: no `memory` export on guest");
            return ERR_INTERNAL;
        }
    };

    let key = match read_validated_key(&mut caller, &memory, key_ptr, key_len) {
        Ok(k) => k,
        Err(code) => return code,
    };
    let id = match read_utf8_arg(
        &mut caller,
        &memory,
        id_ptr,
        id_len,
        MAX_RGA_ID_LEN,
        "crdt_rga_delete",
        "id",
    ) {
        Ok(id) => id,
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
            handle.metrics().record_sync_error();
            tracing::warn!(error = %e, "crdt_rga_delete: broadcast queue full");
            return ERR_INTERNAL;
        }
    };

    {
        let rgas_arc = handle.rgas();
        let mut rgas = rgas_arc.write().await;
        let mut rga = rgas.get(&key).cloned().unwrap_or_else(Rga::new);
        rga.delete(id.clone());
        if let Err(e) = persist_rga_state(&handle.store(), &key, &rga) {
            handle.metrics().record_sync_error();
            tracing::warn!(error = %e, "crdt_rga_delete: failed to persist sequence");
            return ERR_INTERNAL;
        }
        rgas.insert(key.clone(), rga);
    }

    let op = Op::rga_delete(handle.node_id().clone(), key, id);
    tracing::debug!(op_id = %op.id, "queued local RGA delete");
    op_permit.send(op);
    handle.metrics().record_ops(1);

    0
}

async fn crdt_rga_values_impl(
    mut caller: Caller<'_, HostState>,
    key_ptr: u32,
    key_len: u32,
    out_ptr: u32,
    out_cap: u32,
) -> i32 {
    let memory = match get_memory(&mut caller) {
        Some(m) => m,
        None => {
            eprintln!("[nx-core] crdt_rga_values: no `memory` export on guest");
            return ERR_INTERNAL;
        }
    };

    if out_cap > MAX_OUT_CAP {
        eprintln!(
            "[nx-core] crdt_rga_values: output capacity too large: {out_cap} (max {MAX_OUT_CAP})"
        );
        return ERR_INTERNAL;
    }

    let key = match read_validated_key(&mut caller, &memory, key_ptr, key_len) {
        Ok(k) => k,
        Err(code) => return code,
    };

    let handle = match caller.data().sync_handle.as_ref() {
        Some(h) => h.clone(),
        None => return ERR_SYNC_DISABLED,
    };

    let values = {
        let rgas_arc = handle.rgas();
        let rgas = rgas_arc.read().await;
        rgas.get(&key).map(|rga| rga.values()).unwrap_or_default()
    };
    let encoded = match encode_bytes_list(&values) {
        Ok(encoded) => encoded,
        Err(code) => return code,
    };

    if encoded.len() > out_cap as usize {
        return ERR_BUF_TOO_SMALL;
    }

    if let Err(e) = memory.write(&mut caller, out_ptr as usize, &encoded) {
        eprintln!("[nx-core] crdt_rga_values: failed to write output: {e}");
        return ERR_INTERNAL;
    }

    encoded.len() as i32
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

    linker.func_wrap_async(
        "nx",
        "crdt_pncounter_inc",
        |caller: Caller<'_, HostState>, (key_ptr, key_len, delta): (u32, u32, u64)| {
            Box::new(crdt_pncounter_inc_impl(caller, key_ptr, key_len, delta))
        },
    )?;

    linker.func_wrap_async(
        "nx",
        "crdt_pncounter_dec",
        |caller: Caller<'_, HostState>, (key_ptr, key_len, delta): (u32, u32, u64)| {
            Box::new(crdt_pncounter_dec_impl(caller, key_ptr, key_len, delta))
        },
    )?;

    linker.func_wrap_async(
        "nx",
        "crdt_pncounter_value",
        |caller: Caller<'_, HostState>,
         (key_ptr, key_len, out_ptr, out_cap): (u32, u32, u32, u32)| {
            Box::new(crdt_pncounter_value_impl(
                caller, key_ptr, key_len, out_ptr, out_cap,
            ))
        },
    )?;

    linker.func_wrap_async(
        "nx",
        "crdt_lww_set",
        |caller: Caller<'_, HostState>,
         (key_ptr, key_len, value_ptr, value_len): (u32, u32, u32, u32)| {
            Box::new(crdt_lww_set_impl(
                caller, key_ptr, key_len, value_ptr, value_len,
            ))
        },
    )?;

    linker.func_wrap_async(
        "nx",
        "crdt_lww_get",
        |caller: Caller<'_, HostState>,
         (key_ptr, key_len, out_ptr, out_cap): (u32, u32, u32, u32)| {
            Box::new(crdt_lww_get_impl(
                caller, key_ptr, key_len, out_ptr, out_cap,
            ))
        },
    )?;

    linker.func_wrap_async(
        "nx",
        "crdt_lww_map_set",
        |caller: Caller<'_, HostState>,
         (key_ptr, key_len, field_ptr, field_len, value_ptr, value_len): (
            u32,
            u32,
            u32,
            u32,
            u32,
            u32,
        )| {
            Box::new(crdt_lww_map_set_impl(
                caller, key_ptr, key_len, field_ptr, field_len, value_ptr, value_len,
            ))
        },
    )?;

    linker.func_wrap_async(
        "nx",
        "crdt_lww_map_remove",
        |caller: Caller<'_, HostState>,
         (key_ptr, key_len, field_ptr, field_len): (u32, u32, u32, u32)| {
            Box::new(crdt_lww_map_remove_impl(
                caller, key_ptr, key_len, field_ptr, field_len,
            ))
        },
    )?;

    linker.func_wrap_async(
        "nx",
        "crdt_lww_map_get",
        |caller: Caller<'_, HostState>,
         (key_ptr, key_len, field_ptr, field_len, out_ptr, out_cap): (
            u32,
            u32,
            u32,
            u32,
            u32,
            u32,
        )| {
            Box::new(crdt_lww_map_get_impl(
                caller, key_ptr, key_len, field_ptr, field_len, out_ptr, out_cap,
            ))
        },
    )?;

    linker.func_wrap_async(
        "nx",
        "crdt_lww_map_contains",
        |caller: Caller<'_, HostState>,
         (key_ptr, key_len, field_ptr, field_len): (u32, u32, u32, u32)| {
            Box::new(crdt_lww_map_contains_impl(
                caller, key_ptr, key_len, field_ptr, field_len,
            ))
        },
    )?;

    linker.func_wrap_async(
        "nx",
        "crdt_lww_map_entries",
        |caller: Caller<'_, HostState>,
         (key_ptr, key_len, out_ptr, out_cap): (u32, u32, u32, u32)| {
            Box::new(crdt_lww_map_entries_impl(
                caller, key_ptr, key_len, out_ptr, out_cap,
            ))
        },
    )?;

    linker.func_wrap_async(
        "nx",
        "crdt_orset_add",
        |caller: Caller<'_, HostState>,
         (key_ptr, key_len, element_ptr, element_len): (u32, u32, u32, u32)| {
            Box::new(crdt_orset_add_impl(
                caller,
                key_ptr,
                key_len,
                element_ptr,
                element_len,
            ))
        },
    )?;

    linker.func_wrap_async(
        "nx",
        "crdt_orset_remove",
        |caller: Caller<'_, HostState>,
         (key_ptr, key_len, element_ptr, element_len): (u32, u32, u32, u32)| {
            Box::new(crdt_orset_remove_impl(
                caller,
                key_ptr,
                key_len,
                element_ptr,
                element_len,
            ))
        },
    )?;

    linker.func_wrap_async(
        "nx",
        "crdt_orset_contains",
        |caller: Caller<'_, HostState>,
         (key_ptr, key_len, element_ptr, element_len): (u32, u32, u32, u32)| {
            Box::new(crdt_orset_contains_impl(
                caller,
                key_ptr,
                key_len,
                element_ptr,
                element_len,
            ))
        },
    )?;

    linker.func_wrap_async(
        "nx",
        "crdt_orset_elements",
        |caller: Caller<'_, HostState>,
         (key_ptr, key_len, out_ptr, out_cap): (u32, u32, u32, u32)| {
            Box::new(crdt_orset_elements_impl(
                caller, key_ptr, key_len, out_ptr, out_cap,
            ))
        },
    )?;

    linker.func_wrap_async(
        "nx",
        "crdt_rga_insert",
        |caller: Caller<'_, HostState>,
         (
            key_ptr,
            key_len,
            parent_ptr,
            parent_len,
            value_ptr,
            value_len,
            out_id_ptr,
            out_id_cap,
        ): (u32, u32, u32, u32, u32, u32, u32, u32)| {
            Box::new(crdt_rga_insert_impl(
                caller,
                RgaInsertArgs {
                    key_ptr,
                    key_len,
                    parent_ptr,
                    parent_len,
                    value_ptr,
                    value_len,
                    out_id_ptr,
                    out_id_cap,
                },
            ))
        },
    )?;

    linker.func_wrap_async(
        "nx",
        "crdt_rga_delete",
        |caller: Caller<'_, HostState>,
         (key_ptr, key_len, id_ptr, id_len): (u32, u32, u32, u32)| {
            Box::new(crdt_rga_delete_impl(
                caller, key_ptr, key_len, id_ptr, id_len,
            ))
        },
    )?;

    linker.func_wrap_async(
        "nx",
        "crdt_rga_values",
        |caller: Caller<'_, HostState>,
         (key_ptr, key_len, out_ptr, out_cap): (u32, u32, u32, u32)| {
            Box::new(crdt_rga_values_impl(
                caller, key_ptr, key_len, out_ptr, out_cap,
            ))
        },
    )?;

    Ok(())
}
