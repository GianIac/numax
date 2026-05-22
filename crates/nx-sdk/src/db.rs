use crate::__alloc::vec;
use crate::__alloc::vec::Vec;

use crate::error::{NxError, Result};
use crate::ffi;

const ERR_NOT_FOUND: i32 = -1;
const ERR_BUF_TOO_SMALL: i32 = -2;
const ERR_INTERNAL: i32 = -3;
const ERR_RESERVED_KEY: i32 = -4;
const DEFAULT_SCAN_LIMIT: u32 = 64;
const MAX_SCAN_BUFFER: usize = 1024 * 1024;

fn map_rc_unit(rc: i32) -> Result<()> {
    match rc {
        0 => Ok(()),
        ERR_INTERNAL => Err(NxError::Internal),
        ERR_RESERVED_KEY => Err(NxError::ReservedKey),
        c if c < 0 => Err(NxError::UnknownCode(c)),
        _ => Err(NxError::UnknownCode(rc)),
    }
}

/// set(key, value) -> Result<(), NxError>
pub fn set(key: &str, value: &[u8]) -> Result<()> {
    let rc = unsafe {
        ffi::db_set(
            key.as_ptr() as u32,
            key.len() as u32,
            value.as_ptr() as u32,
            value.len() as u32,
        )
    };
    map_rc_unit(rc)
}

/// delete(key) -> Result<(), NxError>
pub fn delete(key: &str) -> Result<()> {
    let rc = unsafe { ffi::db_delete(key.as_ptr() as u32, key.len() as u32) };
    map_rc_unit(rc)
}

/// exists(key) -> Result<bool, NxError>
pub fn exists(key: &str) -> Result<bool> {
    let rc = unsafe { ffi::db_exists(key.as_ptr() as u32, key.len() as u32) };
    match rc {
        0 => Ok(false),
        1 => Ok(true),
        ERR_INTERNAL => Err(NxError::Internal),
        ERR_RESERVED_KEY => Err(NxError::ReservedKey),
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

fn parse_scan_rows(buf: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    let mut offset = 0usize;
    let count = read_u32_le(buf, &mut offset)? as usize;
    let mut rows = Vec::new();

    for _ in 0..count {
        let key_len = read_u32_le(buf, &mut offset)? as usize;
        let value_len = read_u32_le(buf, &mut offset)? as usize;
        let key_end = offset.saturating_add(key_len);
        let value_end = key_end.saturating_add(value_len);
        if key_end > buf.len() || value_end > buf.len() {
            return Err(NxError::Internal);
        }

        rows.push((
            buf[offset..key_end].to_vec(),
            buf[key_end..value_end].to_vec(),
        ));
        offset = value_end;
    }

    if offset != buf.len() {
        return Err(NxError::Internal);
    }

    Ok(rows)
}

/// scan_page(prefix, cursor, limit) -> Result<Vec<(key, value)>, NxError>
pub fn scan_page(prefix: &str, cursor: u64, limit: u32) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    let mut cap: usize = 256;

    loop {
        let mut out = vec![0u8; cap];
        let n = unsafe {
            ffi::db_scan(
                prefix.as_ptr() as u32,
                prefix.len() as u32,
                cursor,
                limit,
                out.as_mut_ptr() as u32,
                out.len() as u32,
            )
        };

        match n {
            ERR_INTERNAL => return Err(NxError::Internal),
            ERR_RESERVED_KEY => return Err(NxError::ReservedKey),
            ERR_BUF_TOO_SMALL => {
                cap = cap.saturating_mul(2);
                if cap > MAX_SCAN_BUFFER {
                    return Err(NxError::BufferTooSmall);
                }
                continue;
            }
            c if c < 0 => return Err(NxError::UnknownCode(c)),
            n => {
                out.truncate(n as usize);
                return parse_scan_rows(&out);
            }
        }
    }
}

/// scan(prefix) -> Result<Vec<(key, value)>, NxError>
pub fn scan(prefix: &str) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    let mut cursor = 0u64;
    let mut out = Vec::new();

    loop {
        let page = scan_page(prefix, cursor, DEFAULT_SCAN_LIMIT)?;
        if page.is_empty() {
            return Ok(out);
        }

        cursor = cursor.saturating_add(page.len() as u64);
        let is_last = page.len() < DEFAULT_SCAN_LIMIT as usize;
        out.extend(page);

        if is_last {
            return Ok(out);
        }
    }
}

// get(key) -> Result<Option<Vec<u8>>, NxError>
pub fn get(key: &str) -> Result<Option<Vec<u8>>> {
    let mut cap: usize = 64;

    loop {
        let mut out = vec![0u8; cap];

        let n = unsafe {
            ffi::db_get(
                key.as_ptr() as u32,
                key.len() as u32,
                out.as_mut_ptr() as u32,
                out.len() as u32,
            )
        };

        match n {
            ERR_NOT_FOUND => return Ok(None),
            ERR_INTERNAL => return Err(NxError::Internal),
            ERR_RESERVED_KEY => return Err(NxError::ReservedKey),
            ERR_BUF_TOO_SMALL => {
                cap = cap.saturating_mul(2);
                if cap > 1024 * 1024 {
                    return Err(NxError::BufferTooSmall);
                }
                continue;
            }
            c if c < 0 => return Err(NxError::UnknownCode(c)),
            n => {
                out.truncate(n as usize);
                return Ok(Some(out));
            }
        }
    }
}
