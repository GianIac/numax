use crate::__alloc::string::{String, ToString};
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
#[must_use = "this SDK call can fail; handle the Result"]
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
#[must_use = "this SDK call can fail; handle the Result"]
pub fn module_id() -> Result<String> {
    let bytes =
        read_dynamic(|out_ptr, out_cap| unsafe { ffi::module_id(out_ptr as u32, out_cap) })?
            .ok_or(NxError::NotFound)?;
    String::from_utf8(bytes).map_err(|_| NxError::Internal)
}

/// Host API capabilities available in the current runtime.
#[must_use = "this SDK call can fail; handle the Result"]
pub fn host_capabilities() -> Result<Vec<String>> {
    let bytes = read_dynamic(|out_ptr, out_cap| unsafe {
        ffi::host_capabilities(out_ptr as u32, out_cap)
    })?
    .ok_or(NxError::NotFound)?;
    let text = core::str::from_utf8(&bytes).map_err(|_| NxError::Internal)?;

    Ok(text
        .split('\n')
        .filter(|capability| !capability.is_empty())
        .map(ToString::to_string)
        .collect())
}

/// Emit a named event to the runtime.
#[must_use = "this SDK call can fail; handle the Result"]
#[inline]
pub fn event_emit(name: &str, payload: &[u8]) -> Result<()> {
    let rc = unsafe {
        ffi::event_emit(
            name.as_ptr() as u32,
            name.len() as u32,
            payload.as_ptr() as u32,
            payload.len() as u32,
        )
    };

    match rc {
        0 => Ok(()),
        ERR_INTERNAL => Err(NxError::Internal),
        c if c < 0 => Err(NxError::UnknownCode(c)),
        _ => Err(NxError::UnknownCode(rc)),
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::NxError;

    fn write_mock_payload(out: *mut u8, out_cap: u32, payload: &[u8]) -> i32 {
        assert!(payload.len() <= out_cap as usize);
        let out = unsafe { core::slice::from_raw_parts_mut(out, out_cap as usize) };
        out[..payload.len()].copy_from_slice(payload);
        payload.len() as i32
    }

    #[test]
    fn read_dynamic_returns_none_on_not_found() {
        assert_eq!(read_dynamic(|_, _| ERR_NOT_FOUND), Ok(None));
    }

    #[test]
    fn read_dynamic_maps_internal_error() {
        assert_eq!(read_dynamic(|_, _| ERR_INTERNAL), Err(NxError::Internal));
    }

    #[test]
    fn read_dynamic_maps_unknown_negative_codes() {
        assert_eq!(read_dynamic(|_, _| -42), Err(NxError::UnknownCode(-42)));
    }

    #[test]
    fn read_dynamic_truncates_to_reported_length() {
        let got = read_dynamic(|out, cap| {
            assert_eq!(cap, 64);
            write_mock_payload(out, cap, b"hello")
        })
        .unwrap()
        .unwrap();
        assert_eq!(got, b"hello");
    }

    #[test]
    fn read_dynamic_retries_with_larger_buffer_after_buf_too_small() {
        let mut calls = 0u32;
        let got = read_dynamic(|out, cap| {
            calls += 1;
            if calls == 1 {
                assert_eq!(cap, 64);
                ERR_BUF_TOO_SMALL
            } else {
                assert_eq!(cap, 128);
                write_mock_payload(out, cap, b"expanded")
            }
        })
        .unwrap()
        .unwrap();
        assert_eq!(got, b"expanded");
        assert_eq!(calls, 2);
    }

    #[test]
    fn read_dynamic_errors_when_buffer_cap_exceeds_max() {
        let mut attempts = 0u32;
        let mut last_cap = 0u32;
        let err = read_dynamic(|_, cap| {
            attempts += 1;
            last_cap = cap;
            ERR_BUF_TOO_SMALL
        })
        .unwrap_err();
        assert_eq!(err, NxError::BufferTooSmall);
        assert!(attempts > 1);
        assert_eq!(last_cap as usize, MAX_SYSTEM_BUFFER);
    }

    /// Mirrors `host_capabilities` newline splitting (empty lines dropped).
    fn decode_capability_list(bytes: &[u8]) -> Result<Vec<String>> {
        let text = core::str::from_utf8(bytes).map_err(|_| NxError::Internal)?;
        Ok(text
            .split('\n')
            .filter(|capability| !capability.is_empty())
            .map(ToString::to_string)
            .collect())
    }

    #[test]
    fn host_capabilities_decode_splits_non_empty_lines() {
        let caps = decode_capability_list(b"net.sync\ndb.kv\nlog.emit").unwrap();
        assert_eq!(
            caps,
            vec![
                "net.sync".to_string(),
                "db.kv".to_string(),
                "log.emit".to_string(),
            ]
        );
    }

    #[test]
    fn host_capabilities_decode_skips_blank_lines() {
        let caps = decode_capability_list(b"net.sync\n\n\nlog.emit\n").unwrap();
        assert_eq!(caps, vec!["net.sync".to_string(), "log.emit".to_string()]);
    }

    #[test]
    fn host_capabilities_decode_rejects_invalid_utf8() {
        assert_eq!(
            decode_capability_list(&[0xff, 0xfe, 0xfd]),
            Err(NxError::Internal)
        );
    }

    /// Mirrors `module_id` UTF-8 decoding after a successful buffer read.
    fn decode_module_id_bytes(bytes: Vec<u8>) -> Result<String> {
        String::from_utf8(bytes).map_err(|_| NxError::Internal)
    }

    #[test]
    fn module_id_decode_accepts_valid_utf8() {
        assert_eq!(
            decode_module_id_bytes(b"my-module".to_vec()),
            Ok("my-module".to_string())
        );
    }

    #[test]
    fn module_id_decode_rejects_invalid_utf8() {
        assert_eq!(
            decode_module_id_bytes(vec![0xff, 0xfe]),
            Err(NxError::Internal)
        );
    }

    /// Mirrors `event_emit` return-code mapping.
    fn map_event_emit_rc(rc: i32) -> Result<()> {
        match rc {
            0 => Ok(()),
            ERR_INTERNAL => Err(NxError::Internal),
            c if c < 0 => Err(NxError::UnknownCode(c)),
            _ => Err(NxError::UnknownCode(rc)),
        }
    }

    #[test]
    fn event_emit_maps_success_and_errors() {
        assert_eq!(map_event_emit_rc(0), Ok(()));
        assert_eq!(map_event_emit_rc(ERR_INTERNAL), Err(NxError::Internal));
        assert_eq!(map_event_emit_rc(-7), Err(NxError::UnknownCode(-7)));
        assert_eq!(map_event_emit_rc(1), Err(NxError::UnknownCode(1)));
    }
}
