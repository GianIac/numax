use crate::__alloc::vec;
use crate::__alloc::vec::Vec;

use crate::{error::NxError, ffi};

const ERR_NOT_FOUND: i32 = -1;
const ERR_BUF_TOO_SMALL: i32 = -2;
const ERR_INTERNAL: i32 = -3;

/// set(key, value) -> Result<(), NxError>
pub fn set(key: &str, value: &[u8]) -> Result<(), NxError> {
    let rc = unsafe {
        ffi::db_set(
            key.as_ptr() as i32,
            key.len() as i32,
            value.as_ptr() as i32,
            value.len() as i32,
        )
    };

    if rc == 0 {
        Ok(())
    } else {
        Err(NxError::Internal)
    }
}

/// delete(key) -> Result<(), NxError>
pub fn delete(key: &str) -> Result<(), NxError> {
    let rc = unsafe { ffi::db_delete(key.as_ptr() as i32, key.len() as i32) };

    if rc == 0 {
        Ok(())
    } else {
        Err(NxError::Internal)
    }
}

/// get(key) -> Result<Option<Vec<u8>>, NxError>
/// - None => key missing
/// - Some(bytes) => value
/// Gestisce automaticamente il caso buffer too small (-2) riallocando e riprovando.
pub fn get(key: &str) -> Result<Option<Vec<u8>>, NxError> {
    let mut cap: usize = 64;

    loop {
        // vec! macro è disponibile perché importata da crate::__alloc::vec
        let mut out = vec![0u8; cap];

        let n = unsafe {
            ffi::db_get(
                key.as_ptr() as i32,
                key.len() as i32,
                out.as_mut_ptr() as i32,
                out.len() as i32,
            )
        };

        if n == ERR_NOT_FOUND {
            return Ok(None);
        }
        if n == ERR_INTERNAL {
            return Err(NxError::Internal);
        }
        if n == ERR_BUF_TOO_SMALL {
            cap = cap.saturating_mul(2);
            if cap > 1024 * 1024 {
                return Err(NxError::BufferTooSmall);
            }
            continue;
        }
        if n < 0 {
            return Err(NxError::Internal);
        }

        out.truncate(n as usize);
        return Ok(Some(out));
    }
}
