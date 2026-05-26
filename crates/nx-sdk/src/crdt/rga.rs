use crate::__alloc::string::String;
use crate::__alloc::vec;
use crate::__alloc::vec::Vec;

use crate::error::{NxError, Result};
use crate::ffi;

const ERR_BUF_TOO_SMALL: i32 = -2;
const ERR_INTERNAL: i32 = -3;
const ERR_RESERVED_KEY: i32 = -4;
const ERR_SYNC_DISABLED: i32 = -5;
const MAX_RGA_BUFFER: usize = 1024 * 1024;

/// Insert `value` after `parent` in the RGA stored under `key`.
///
/// Returns the generated element id. Keep this id if the element may need to be
/// deleted later or used as parent for another insertion.
pub fn insert_after(key: &str, parent: Option<&str>, value: &[u8]) -> Result<String> {
    let (parent_ptr, parent_len) = match parent {
        Some(parent) => (parent.as_ptr() as u32, parent.len() as u32),
        None => (0, 0),
    };
    let mut cap: usize = 64;

    loop {
        let mut out = vec![0u8; cap];
        let n = unsafe {
            ffi::crdt_rga_insert(
                key.as_ptr() as u32,
                key.len() as u32,
                parent_ptr,
                parent_len,
                value.as_ptr() as u32,
                value.len() as u32,
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
                if cap > MAX_RGA_BUFFER {
                    return Err(NxError::BufferTooSmall);
                }
            }
            c if c < 0 => return Err(NxError::UnknownCode(c)),
            n => {
                out.truncate(n as usize);
                return String::from_utf8(out).map_err(|_| NxError::Internal);
            }
        }
    }
}

/// Delete the element identified by `id`.
pub fn delete(key: &str, id: &str) -> Result<()> {
    let rc = unsafe {
        ffi::crdt_rga_delete(
            key.as_ptr() as u32,
            key.len() as u32,
            id.as_ptr() as u32,
            id.len() as u32,
        )
    };
    map_unit_result(rc)
}

/// Return the currently visible values in deterministic sequence order.
pub fn values(key: &str) -> Result<Vec<Vec<u8>>> {
    let mut cap: usize = 256;

    loop {
        let mut out = vec![0u8; cap];
        let n = unsafe {
            ffi::crdt_rga_values(
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
                if cap > MAX_RGA_BUFFER {
                    return Err(NxError::BufferTooSmall);
                }
            }
            c if c < 0 => return Err(NxError::UnknownCode(c)),
            n => {
                out.truncate(n as usize);
                return parse_values(&out);
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

fn parse_values(buf: &[u8]) -> Result<Vec<Vec<u8>>> {
    let mut offset = 0usize;
    let count = read_u32_le(buf, &mut offset)? as usize;
    let mut values = Vec::new();

    for _ in 0..count {
        let value_len = read_u32_le(buf, &mut offset)? as usize;
        let value_end = offset.saturating_add(value_len);
        if value_end > buf.len() {
            return Err(NxError::Internal);
        }
        values.push(buf[offset..value_end].to_vec());
        offset = value_end;
    }

    if offset != buf.len() {
        return Err(NxError::Internal);
    }

    Ok(values)
}
