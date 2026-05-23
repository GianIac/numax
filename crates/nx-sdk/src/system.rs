use crate::__alloc::string::String;
use crate::__alloc::vec;
use crate::__alloc::vec::Vec;

use crate::error::{NxError, Result};
use crate::ffi;

const ERR_NOT_FOUND: i32 = -1;
const ERR_BUF_TOO_SMALL: i32 = -2;
const ERR_INTERNAL: i32 = -3;
const MAX_SYSTEM_BUFFER: usize = 1024 * 1024;

fn read_dynamic(mut call: impl FnMut(*mut u8, u32) -> i32) -> Result<Option<Vec<u8>>> {
    let mut cap = 64usize;

    loop {
        let mut out = vec![0u8; cap];
        let rc = call(out.as_mut_ptr(), out.len() as u32);

        match rc {
            ERR_NOT_FOUND => return Ok(None),
            ERR_INTERNAL => return Err(NxError::Internal),
            ERR_BUF_TOO_SMALL => {
                cap = cap.saturating_mul(2);
                if cap > MAX_SYSTEM_BUFFER {
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

/// Read an allowed environment variable from the host.
///
/// The host currently exposes only `NX_*` and `NUMAX_*` uppercase variables.
pub fn env_get(key: &str) -> Result<Option<Vec<u8>>> {
    read_dynamic(|out_ptr, out_cap| unsafe {
        ffi::env_get(
            key.as_ptr() as u32,
            key.len() as u32,
            out_ptr as u32,
            out_cap,
        )
    })
}

/// Identifier of the current module as provided by the runtime.
pub fn module_id() -> Result<String> {
    let bytes =
        read_dynamic(|out_ptr, out_cap| unsafe { ffi::module_id(out_ptr as u32, out_cap) })?
            .ok_or(NxError::NotFound)?;
    String::from_utf8(bytes).map_err(|_| NxError::Internal)
}

/// Abort guest execution with a host-visible error message.
pub fn abort(message: &str) -> ! {
    unsafe {
        ffi::abort(message.as_ptr() as u32, message.len() as u32);
    }

    loop {
        core::hint::spin_loop();
    }
}
