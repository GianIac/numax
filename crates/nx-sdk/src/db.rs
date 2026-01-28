use crate::__alloc::vec;
use crate::__alloc::vec::Vec;

use crate::error::{NxError, Result};
use crate::ffi;

const ERR_NOT_FOUND: i32 = -1;
const ERR_BUF_TOO_SMALL: i32 = -2;
const ERR_INTERNAL: i32 = -3;

fn map_rc_unit(rc: i32) -> Result<()> {
    match rc {
        0 => Ok(()),
        ERR_INTERNAL => Err(NxError::Internal),
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

/// get(key) -> Result<Option<Vec<u8>>, NxError>
/// - Ok(None) => key missing
/// - Ok(Some(bytes)) => value
/// Gestisce automaticamente il caso buffer too small (-2) riallocando e riprovando.
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
