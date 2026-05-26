use crate::__alloc::string::String;
use crate::__alloc::vec;
use crate::__alloc::vec::Vec;

use crate::error::{NxError, Result};
use crate::ffi;

const ERR_NOT_FOUND: i32 = -1;
const ERR_BUF_TOO_SMALL: i32 = -2;
const ERR_INTERNAL: i32 = -3;
const ERR_RESERVED_KEY: i32 = -4;
const ERR_SYNC_DISABLED: i32 = -5;
const MAX_LWW_MAP_BUFFER: usize = 1024 * 1024;

/// Set `field` in the LWW-Map under `key`.
///
/// The host assigns the timestamp and local writer NodeId.
pub fn set(key: &str, field: &str, value: &[u8]) -> Result<()> {
    let rc = unsafe {
        ffi::crdt_lww_map_set(
            key.as_ptr() as u32,
            key.len() as u32,
            field.as_ptr() as u32,
            field.len() as u32,
            value.as_ptr() as u32,
            value.len() as u32,
        )
    };
    map_unit_result(rc)
}

/// Remove `field` from the LWW-Map under `key`.
pub fn remove(key: &str, field: &str) -> Result<()> {
    let rc = unsafe {
        ffi::crdt_lww_map_remove(
            key.as_ptr() as u32,
            key.len() as u32,
            field.as_ptr() as u32,
            field.len() as u32,
        )
    };
    map_unit_result(rc)
}

/// Read the current winning value for `field` in the LWW-Map under `key`.
pub fn get(key: &str, field: &str) -> Result<Option<Vec<u8>>> {
    let mut cap: usize = 256;

    loop {
        let mut out = vec![0u8; cap];
        let n = unsafe {
            ffi::crdt_lww_map_get(
                key.as_ptr() as u32,
                key.len() as u32,
                field.as_ptr() as u32,
                field.len() as u32,
                out.as_mut_ptr() as u32,
                out.len() as u32,
            )
        };

        match n {
            ERR_NOT_FOUND => return Ok(None),
            ERR_INTERNAL => return Err(NxError::Internal),
            ERR_RESERVED_KEY => return Err(NxError::ReservedKey),
            ERR_SYNC_DISABLED => return Err(NxError::SyncDisabled),
            ERR_BUF_TOO_SMALL => {
                cap = cap.saturating_mul(2);
                if cap > MAX_LWW_MAP_BUFFER {
                    return Err(NxError::BufferTooSmall);
                }
            }
            c if c < 0 => return Err(NxError::UnknownCode(c)),
            n => {
                out.truncate(n as usize);
                return Ok(Some(out));
            }
        }
    }
}

/// Return true when `field` currently has a visible value.
pub fn contains(key: &str, field: &str) -> Result<bool> {
    let rc = unsafe {
        ffi::crdt_lww_map_contains(
            key.as_ptr() as u32,
            key.len() as u32,
            field.as_ptr() as u32,
            field.len() as u32,
        )
    };

    match rc {
        0 => Ok(false),
        1 => Ok(true),
        ERR_INTERNAL => Err(NxError::Internal),
        ERR_RESERVED_KEY => Err(NxError::ReservedKey),
        ERR_SYNC_DISABLED => Err(NxError::SyncDisabled),
        c if c < 0 => Err(NxError::UnknownCode(c)),
        _ => Err(NxError::Internal),
    }
}

/// Return visible LWW-Map entries in deterministic field order.
pub fn entries(key: &str) -> Result<Vec<(String, Vec<u8>)>> {
    let mut cap: usize = 256;

    loop {
        let mut out = vec![0u8; cap];
        let n = unsafe {
            ffi::crdt_lww_map_entries(
                key.as_ptr() as u32,
                key.len() as u32,
                out.as_mut_ptr() as u32,
                out.len() as u32,
            )
        };

        match n {
            ERR_INTERNAL => return Err(NxError::Internal),
            ERR_RESERVED_KEY => return Err(NxError::ReservedKey),
            ERR_SYNC_DISABLED => return Err(NxError::SyncDisabled),
            ERR_BUF_TOO_SMALL => {
                cap = cap.saturating_mul(2);
                if cap > MAX_LWW_MAP_BUFFER {
                    return Err(NxError::BufferTooSmall);
                }
            }
            c if c < 0 => return Err(NxError::UnknownCode(c)),
            n => {
                out.truncate(n as usize);
                return parse_entries(&out);
            }
        }
    }
}

fn map_unit_result(rc: i32) -> Result<()> {
    match rc {
        0 => Ok(()),
        ERR_INTERNAL => Err(NxError::Internal),
        ERR_RESERVED_KEY => Err(NxError::ReservedKey),
        ERR_SYNC_DISABLED => Err(NxError::SyncDisabled),
        c if c < 0 => Err(NxError::UnknownCode(c)),
        _ => Err(NxError::UnknownCode(rc)),
    }
}

fn read_u32_le(buf: &[u8], offset: &mut usize) -> Result<u32> {
    let end = offset.saturating_add(4);
    if end > buf.len() {
        return Err(NxError::Internal);
    }

    let value = u32::from_le_bytes([
        buf[*offset],
        buf[*offset + 1],
        buf[*offset + 2],
        buf[*offset + 3],
    ]);
    *offset = end;
    Ok(value)
}

fn parse_entries(buf: &[u8]) -> Result<Vec<(String, Vec<u8>)>> {
    let mut offset = 0usize;
    let count = read_u32_le(buf, &mut offset)? as usize;
    let mut entries = Vec::new();

    for _ in 0..count {
        let field_len = read_u32_le(buf, &mut offset)? as usize;
        let field_end = offset.saturating_add(field_len);
        if field_end > buf.len() {
            return Err(NxError::Internal);
        }
        let field =
            String::from_utf8(buf[offset..field_end].to_vec()).map_err(|_| NxError::Internal)?;
        offset = field_end;

        let value_len = read_u32_le(buf, &mut offset)? as usize;
        let value_end = offset.saturating_add(value_len);
        if value_end > buf.len() {
            return Err(NxError::Internal);
        }
        let value = buf[offset..value_end].to_vec();
        offset = value_end;

        entries.push((field, value));
    }

    if offset != buf.len() {
        return Err(NxError::Internal);
    }

    Ok(entries)
}
