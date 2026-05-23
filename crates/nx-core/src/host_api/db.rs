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
const MAX_SCAN_LIMIT: u32 = 1024;

const MAX_READ_LEN: u32 = 1024 * 1024; // 1 MiB

#[derive(Clone, Copy)]
struct KeyCursorPageRequest {
    prefix_ptr: u32,
    prefix_len: u32,
    start_after_ptr: u32,
    start_after_len: u32,
    limit: u32,
    out_ptr: u32,
    out_cap: u32,
}

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

fn read_validated_key(
    caller: &mut Caller<'_, HostState>,
    memory: &Memory,
    key_ptr: u32,
    key_len: u32,
    api_name: &str,
) -> std::result::Result<Vec<u8>, i32> {
    if key_len > MAX_KEY_LEN {
        eprintln!("[nx-core] {api_name}: invalid key length: {key_len} (max {MAX_KEY_LEN})");
        return Err(ERR_INTERNAL);
    }

    let key = match read_bytes(caller, memory, key_ptr, key_len) {
        Ok(k) => k,
        Err(e) => {
            eprintln!("[nx-core] {api_name}: failed to read key: {e}");
            return Err(ERR_INTERNAL);
        }
    };

    if key.starts_with(crate::host_api::crdt::RESERVED_PREFIX.as_bytes()) {
        return Err(ERR_RESERVED_KEY);
    }

    Ok(key)
}

fn read_scan_cursor(
    caller: &mut Caller<'_, HostState>,
    memory: &Memory,
    cursor_ptr: u32,
    cursor_len: u32,
    prefix: &[u8],
    api_name: &str,
) -> std::result::Result<Option<Vec<u8>>, i32> {
    if cursor_len == 0 {
        return Ok(None);
    }
    if cursor_len > MAX_KEY_LEN {
        eprintln!("[nx-core] {api_name}: invalid cursor length: {cursor_len} (max {MAX_KEY_LEN})");
        return Err(ERR_INTERNAL);
    }

    let cursor = match read_bytes(caller, memory, cursor_ptr, cursor_len) {
        Ok(cursor) => cursor,
        Err(e) => {
            eprintln!("[nx-core] {api_name}: failed to read cursor: {e}");
            return Err(ERR_INTERNAL);
        }
    };

    if cursor.starts_with(crate::host_api::crdt::RESERVED_PREFIX.as_bytes()) {
        return Err(ERR_RESERVED_KEY);
    }
    if !prefix.is_empty() && !cursor.starts_with(prefix) {
        eprintln!("[nx-core] {api_name}: cursor is outside the requested prefix");
        return Err(ERR_INTERNAL);
    }

    Ok(Some(cursor))
}

fn encode_scan_rows(rows: &[(Vec<u8>, Vec<u8>)]) -> std::result::Result<Vec<u8>, i32> {
    let row_count = u32::try_from(rows.len()).map_err(|_| ERR_INTERNAL)?;
    let mut out = Vec::new();
    out.extend_from_slice(&row_count.to_le_bytes());

    for (key, value) in rows {
        let key_len = u32::try_from(key.len()).map_err(|_| ERR_INTERNAL)?;
        let value_len = u32::try_from(value.len()).map_err(|_| ERR_INTERNAL)?;
        out.extend_from_slice(&key_len.to_le_bytes());
        out.extend_from_slice(&value_len.to_le_bytes());
        out.extend_from_slice(key);
        out.extend_from_slice(value);
    }

    Ok(out)
}

fn encode_keys(keys: &[Vec<u8>]) -> std::result::Result<Vec<u8>, i32> {
    let key_count = u32::try_from(keys.len()).map_err(|_| ERR_INTERNAL)?;
    let mut out = Vec::new();
    out.extend_from_slice(&key_count.to_le_bytes());

    for key in keys {
        let key_len = u32::try_from(key.len()).map_err(|_| ERR_INTERNAL)?;
        out.extend_from_slice(&key_len.to_le_bytes());
        out.extend_from_slice(key);
    }

    Ok(out)
}

fn validate_page_request(api_name: &str, limit: u32, out_cap: u32) -> std::result::Result<(), i32> {
    if limit == 0 || limit > MAX_SCAN_LIMIT {
        eprintln!("[nx-core] {api_name}: invalid limit: {limit} (max {MAX_SCAN_LIMIT})");
        return Err(ERR_INTERNAL);
    }
    if out_cap > MAX_OUT_CAP {
        eprintln!("[nx-core] {api_name}: output capacity too large: {out_cap} (max {MAX_OUT_CAP})");
        return Err(ERR_INTERNAL);
    }

    Ok(())
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

    if out_cap > MAX_OUT_CAP {
        eprintln!("[nx-core] db_get: output capacity too large: {out_cap} (max {MAX_OUT_CAP})");
        return ERR_INTERNAL;
    }

    let key = match read_validated_key(&mut caller, &memory, key_ptr, key_len, "db_get") {
        Ok(k) => k,
        Err(code) => return code,
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

    if val_len > MAX_VALUE_LEN {
        eprintln!("[nx-core] db_set: invalid value length: {val_len} (max {MAX_VALUE_LEN})");
        return ERR_INTERNAL;
    }

    let key = match read_validated_key(&mut caller, &memory, key_ptr, key_len, "db_set") {
        Ok(k) => k,
        Err(code) => return code,
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
}

fn db_delete_impl(mut caller: Caller<'_, HostState>, key_ptr: u32, key_len: u32) -> i32 {
    let memory = match get_memory(&mut caller) {
        Some(m) => m,
        None => {
            eprintln!("[nx-core] db_delete: no `memory` export on guest");
            return ERR_INTERNAL;
        }
    };

    let key = match read_validated_key(&mut caller, &memory, key_ptr, key_len, "db_delete") {
        Ok(k) => k,
        Err(code) => return code,
    };

    if let Err(e) = caller.data().store.delete(&key) {
        eprintln!("[nx-core] db_delete: store error: {e}");
        return ERR_INTERNAL;
    }

    0
}

fn db_exists_impl(mut caller: Caller<'_, HostState>, key_ptr: u32, key_len: u32) -> i32 {
    let memory = match get_memory(&mut caller) {
        Some(m) => m,
        None => {
            eprintln!("[nx-core] db_exists: no `memory` export on guest");
            return ERR_INTERNAL;
        }
    };

    let key = match read_validated_key(&mut caller, &memory, key_ptr, key_len, "db_exists") {
        Ok(k) => k,
        Err(code) => return code,
    };

    match caller.data().store.exists(&key) {
        Ok(true) => 1,
        Ok(false) => 0,
        Err(e) => {
            eprintln!("[nx-core] db_exists: store error: {e}");
            ERR_INTERNAL
        }
    }
}

fn db_scan_impl(
    mut caller: Caller<'_, HostState>,
    prefix_ptr: u32,
    prefix_len: u32,
    cursor: u64,
    limit: u32,
    out_ptr: u32,
    out_cap: u32,
) -> i32 {
    let memory = match get_memory(&mut caller) {
        Some(m) => m,
        None => {
            eprintln!("[nx-core] db_scan: no `memory` export on guest");
            return ERR_INTERNAL;
        }
    };

    if let Err(code) = validate_page_request("db_scan", limit, out_cap) {
        return code;
    }

    let prefix = match read_validated_key(&mut caller, &memory, prefix_ptr, prefix_len, "db_scan") {
        Ok(k) => k,
        Err(code) => return code,
    };

    let rows = match caller.data().store.scan_prefix_page(
        &prefix,
        cursor,
        limit,
        Some(crate::host_api::crdt::RESERVED_PREFIX.as_bytes()),
    ) {
        Ok(rows) => rows,
        Err(e) => {
            eprintln!("[nx-core] db_scan: store error: {e}");
            return ERR_INTERNAL;
        }
    };

    let encoded = match encode_scan_rows(&rows) {
        Ok(encoded) => encoded,
        Err(code) => return code,
    };

    if encoded.len() > out_cap as usize {
        return ERR_BUF_TOO_SMALL;
    }

    if let Err(e) = memory.write(&mut caller, out_ptr as usize, &encoded) {
        eprintln!("[nx-core] db_scan: failed to write output: {e}");
        return ERR_INTERNAL;
    }

    encoded.len() as i32
}

fn db_scan_after_impl(mut caller: Caller<'_, HostState>, req: KeyCursorPageRequest) -> i32 {
    let memory = match get_memory(&mut caller) {
        Some(m) => m,
        None => {
            eprintln!("[nx-core] db_scan_after: no `memory` export on guest");
            return ERR_INTERNAL;
        }
    };

    if let Err(code) = validate_page_request("db_scan_after", req.limit, req.out_cap) {
        return code;
    }

    let prefix = match read_validated_key(
        &mut caller,
        &memory,
        req.prefix_ptr,
        req.prefix_len,
        "db_scan_after",
    ) {
        Ok(k) => k,
        Err(code) => return code,
    };
    let start_after = match read_scan_cursor(
        &mut caller,
        &memory,
        req.start_after_ptr,
        req.start_after_len,
        &prefix,
        "db_scan_after",
    ) {
        Ok(cursor) => cursor,
        Err(code) => return code,
    };

    let rows = match caller.data().store.scan_prefix_page_after(
        &prefix,
        start_after.as_deref(),
        req.limit,
        Some(crate::host_api::crdt::RESERVED_PREFIX.as_bytes()),
    ) {
        Ok(rows) => rows,
        Err(e) => {
            eprintln!("[nx-core] db_scan_after: store error: {e}");
            return ERR_INTERNAL;
        }
    };

    let encoded = match encode_scan_rows(&rows) {
        Ok(encoded) => encoded,
        Err(code) => return code,
    };

    if encoded.len() > req.out_cap as usize {
        return ERR_BUF_TOO_SMALL;
    }

    if let Err(e) = memory.write(&mut caller, req.out_ptr as usize, &encoded) {
        eprintln!("[nx-core] db_scan_after: failed to write output: {e}");
        return ERR_INTERNAL;
    }

    encoded.len() as i32
}

fn db_keys_impl(
    mut caller: Caller<'_, HostState>,
    prefix_ptr: u32,
    prefix_len: u32,
    cursor: u64,
    limit: u32,
    out_ptr: u32,
    out_cap: u32,
) -> i32 {
    let memory = match get_memory(&mut caller) {
        Some(m) => m,
        None => {
            eprintln!("[nx-core] db_keys: no `memory` export on guest");
            return ERR_INTERNAL;
        }
    };

    if let Err(code) = validate_page_request("db_keys", limit, out_cap) {
        return code;
    }

    let prefix = match read_validated_key(&mut caller, &memory, prefix_ptr, prefix_len, "db_keys") {
        Ok(k) => k,
        Err(code) => return code,
    };

    let keys = match caller.data().store.keys_prefix_page(
        &prefix,
        cursor,
        limit,
        Some(crate::host_api::crdt::RESERVED_PREFIX.as_bytes()),
    ) {
        Ok(keys) => keys,
        Err(e) => {
            eprintln!("[nx-core] db_keys: store error: {e}");
            return ERR_INTERNAL;
        }
    };

    let encoded = match encode_keys(&keys) {
        Ok(encoded) => encoded,
        Err(code) => return code,
    };

    if encoded.len() > out_cap as usize {
        return ERR_BUF_TOO_SMALL;
    }

    if let Err(e) = memory.write(&mut caller, out_ptr as usize, &encoded) {
        eprintln!("[nx-core] db_keys: failed to write output: {e}");
        return ERR_INTERNAL;
    }

    encoded.len() as i32
}

fn db_keys_after_impl(mut caller: Caller<'_, HostState>, req: KeyCursorPageRequest) -> i32 {
    let memory = match get_memory(&mut caller) {
        Some(m) => m,
        None => {
            eprintln!("[nx-core] db_keys_after: no `memory` export on guest");
            return ERR_INTERNAL;
        }
    };

    if let Err(code) = validate_page_request("db_keys_after", req.limit, req.out_cap) {
        return code;
    }

    let prefix = match read_validated_key(
        &mut caller,
        &memory,
        req.prefix_ptr,
        req.prefix_len,
        "db_keys_after",
    ) {
        Ok(k) => k,
        Err(code) => return code,
    };
    let start_after = match read_scan_cursor(
        &mut caller,
        &memory,
        req.start_after_ptr,
        req.start_after_len,
        &prefix,
        "db_keys_after",
    ) {
        Ok(cursor) => cursor,
        Err(code) => return code,
    };

    let keys = match caller.data().store.keys_prefix_page_after(
        &prefix,
        start_after.as_deref(),
        req.limit,
        Some(crate::host_api::crdt::RESERVED_PREFIX.as_bytes()),
    ) {
        Ok(keys) => keys,
        Err(e) => {
            eprintln!("[nx-core] db_keys_after: store error: {e}");
            return ERR_INTERNAL;
        }
    };

    let encoded = match encode_keys(&keys) {
        Ok(encoded) => encoded,
        Err(code) => return code,
    };

    if encoded.len() > req.out_cap as usize {
        return ERR_BUF_TOO_SMALL;
    }

    if let Err(e) = memory.write(&mut caller, req.out_ptr as usize, &encoded) {
        eprintln!("[nx-core] db_keys_after: failed to write output: {e}");
        return ERR_INTERNAL;
    }

    encoded.len() as i32
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
        "db_exists",
        |caller: Caller<'_, HostState>, key_ptr: u32, key_len: u32| -> i32 {
            db_exists_impl(caller, key_ptr, key_len)
        },
    )?;

    linker.func_wrap(
        "nx",
        "db_scan",
        |caller: Caller<'_, HostState>,
         prefix_ptr: u32,
         prefix_len: u32,
         cursor: u64,
         limit: u32,
         out_ptr: u32,
         out_cap: u32|
         -> i32 {
            db_scan_impl(
                caller, prefix_ptr, prefix_len, cursor, limit, out_ptr, out_cap,
            )
        },
    )?;

    linker.func_wrap(
        "nx",
        "db_keys",
        |caller: Caller<'_, HostState>,
         prefix_ptr: u32,
         prefix_len: u32,
         cursor: u64,
         limit: u32,
         out_ptr: u32,
         out_cap: u32|
         -> i32 {
            db_keys_impl(
                caller, prefix_ptr, prefix_len, cursor, limit, out_ptr, out_cap,
            )
        },
    )?;

    linker.func_wrap(
        "nx",
        "db_scan_after",
        |caller: Caller<'_, HostState>,
         prefix_ptr: u32,
         prefix_len: u32,
         start_after_ptr: u32,
         start_after_len: u32,
         limit: u32,
         out_ptr: u32,
         out_cap: u32|
         -> i32 {
            db_scan_after_impl(
                caller,
                KeyCursorPageRequest {
                    prefix_ptr,
                    prefix_len,
                    start_after_ptr,
                    start_after_len,
                    limit,
                    out_ptr,
                    out_cap,
                },
            )
        },
    )?;

    linker.func_wrap(
        "nx",
        "db_keys_after",
        |caller: Caller<'_, HostState>,
         prefix_ptr: u32,
         prefix_len: u32,
         start_after_ptr: u32,
         start_after_len: u32,
         limit: u32,
         out_ptr: u32,
         out_cap: u32|
         -> i32 {
            db_keys_after_impl(
                caller,
                KeyCursorPageRequest {
                    prefix_ptr,
                    prefix_len,
                    start_after_ptr,
                    start_after_len,
                    limit,
                    out_ptr,
                    out_cap,
                },
            )
        },
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
