use crate::error::{NxError, Result};
use crate::ffi;

const ERR_BUF_TOO_SMALL: i32 = -2;
const ERR_INTERNAL: i32 = -3;
const ERR_RESERVED_KEY: i32 = -4;
const ERR_SYNC_DISABLED: i32 = -5;

/// Increment the positive side of the PNCounter under `key` by `delta`.
pub fn inc(key: &str, delta: u64) -> Result<()> {
    let rc = unsafe { ffi::crdt_pncounter_inc(key.as_ptr() as u32, key.len() as u32, delta) };
    map_unit_result(rc)
}

/// Increment the negative side of the PNCounter under `key` by `delta`.
pub fn dec(key: &str, delta: u64) -> Result<()> {
    let rc = unsafe { ffi::crdt_pncounter_dec(key.as_ptr() as u32, key.len() as u32, delta) };
    map_unit_result(rc)
}

/// Read the current converged signed value of the PNCounter under `key`.
pub fn value(key: &str) -> Result<i64> {
    let mut buf = [0u8; 8];
    let n = unsafe {
        ffi::crdt_pncounter_value(
            key.as_ptr() as u32,
            key.len() as u32,
            buf.as_mut_ptr() as u32,
            buf.len() as u32,
        )
    };
    match n {
        8 => Ok(i64::from_le_bytes(buf)),
        ERR_INTERNAL => Err(NxError::Internal),
        ERR_BUF_TOO_SMALL => Err(NxError::BufferTooSmall),
        ERR_RESERVED_KEY => Err(NxError::ReservedKey),
        ERR_SYNC_DISABLED => Err(NxError::SyncDisabled),
        c if c < 0 => Err(NxError::UnknownCode(c)),
        _ => Err(NxError::Internal),
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
