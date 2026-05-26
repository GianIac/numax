use crate::__alloc::string::String;
use crate::__alloc::vec;
use crate::__alloc::vec::Vec;

use crate::error::{NxError, Result};
use crate::ffi;

const ERR_BUF_TOO_SMALL: i32 = -2;
const ERR_INTERNAL: i32 = -3;
const ERR_RESERVED_KEY: i32 = -4;
const ERR_SYNC_DISABLED: i32 = -5;
const MAX_ORSET_BUFFER: usize = 1024 * 1024;

/// Add `element` to the ORSet under `key`.
pub fn add(key: &str, element: &str) -> Result<()> {
    let rc = unsafe {
        ffi::crdt_orset_add(
            key.as_ptr() as u32,
            key.len() as u32,
            element.as_ptr() as u32,
            element.len() as u32,
        )
    };
    map_unit_result(rc)
}

/// Remove the locally observed add-tags for `element` from the ORSet under `key`.
pub fn remove(key: &str, element: &str) -> Result<()> {
    let rc = unsafe {
        ffi::crdt_orset_remove(
            key.as_ptr() as u32,
            key.len() as u32,
            element.as_ptr() as u32,
            element.len() as u32,
        )
    };
    map_unit_result(rc)
}

/// Return true when `element` is currently visible in the ORSet under `key`.
pub fn contains(key: &str, element: &str) -> Result<bool> {
    let rc = unsafe {
        ffi::crdt_orset_contains(
            key.as_ptr() as u32,
            key.len() as u32,
            element.as_ptr() as u32,
            element.len() as u32,
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

/// Return visible ORSet elements in deterministic order.
pub fn elements(key: &str) -> Result<Vec<String>> {
    let mut cap: usize = 256;

    loop {
        let mut out = vec![0u8; cap];
        let n = unsafe {
            ffi::crdt_orset_elements(
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
                if cap > MAX_ORSET_BUFFER {
                    return Err(NxError::BufferTooSmall);
                }
            }
            c if c < 0 => return Err(NxError::UnknownCode(c)),
            n => {
                out.truncate(n as usize);
                return parse_elements(&out);
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

fn parse_elements(buf: &[u8]) -> Result<Vec<String>> {
    let mut offset = 0usize;
    let count = read_u32_le(buf, &mut offset)? as usize;
    let mut elements = Vec::new();

    for _ in 0..count {
        let len = read_u32_le(buf, &mut offset)? as usize;
        let end = offset.saturating_add(len);
        if end > buf.len() {
            return Err(NxError::Internal);
        }

        let element =
            String::from_utf8(buf[offset..end].to_vec()).map_err(|_| NxError::Internal)?;
        elements.push(element);
        offset = end;
    }

    if offset != buf.len() {
        return Err(NxError::Internal);
    }

    Ok(elements)
}
