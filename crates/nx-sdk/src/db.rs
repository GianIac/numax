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
#[must_use = "this SDK call can fail; handle the Result"]
#[inline]
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
#[must_use = "this SDK call can fail; handle the Result"]
#[inline]
pub fn delete(key: &str) -> Result<()> {
    let rc = unsafe { ffi::db_delete(key.as_ptr() as u32, key.len() as u32) };
    map_rc_unit(rc)
}

/// exists(key) -> Result<bool, NxError>
#[must_use = "this SDK call can fail; handle the Result"]
#[inline]
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

fn parse_keys(buf: &[u8]) -> Result<Vec<Vec<u8>>> {
    let mut offset = 0usize;
    let count = read_u32_le(buf, &mut offset)? as usize;
    let mut keys = Vec::new();

    for _ in 0..count {
        let key_len = read_u32_le(buf, &mut offset)? as usize;
        let key_end = offset.saturating_add(key_len);
        if key_end > buf.len() {
            return Err(NxError::Internal);
        }

        keys.push(buf[offset..key_end].to_vec());
        offset = key_end;
    }

    if offset != buf.len() {
        return Err(NxError::Internal);
    }

    Ok(keys)
}

/// scan_page(prefix, cursor, limit) -> Result<Vec<(key, value)>, NxError>
#[must_use = "this SDK call can fail; handle the Result"]
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

/// scan_page_after(prefix, start_after_key, limit) -> Result<Vec<(key, value)>, NxError>
#[must_use = "this SDK call can fail; handle the Result"]
pub fn scan_page_after(
    prefix: &str,
    start_after_key: Option<&[u8]>,
    limit: u32,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    let mut cap: usize = 256;
    let start_after = start_after_key.unwrap_or_default();

    loop {
        let mut out = vec![0u8; cap];
        let n = unsafe {
            ffi::db_scan_after(
                prefix.as_ptr() as u32,
                prefix.len() as u32,
                start_after.as_ptr() as u32,
                start_after.len() as u32,
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

/// `keys_page(prefix, cursor, limit) -> Result<Vec<key>, NxError>`
#[must_use = "this SDK call can fail; handle the Result"]
pub fn keys_page(prefix: &str, cursor: u64, limit: u32) -> Result<Vec<Vec<u8>>> {
    let mut cap: usize = 256;

    loop {
        let mut out = vec![0u8; cap];
        let n = unsafe {
            ffi::db_keys(
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
                return parse_keys(&out);
            }
        }
    }
}

/// `keys_page_after(prefix, start_after_key, limit) -> Result<Vec<key>, NxError>`
#[must_use = "this SDK call can fail; handle the Result"]
pub fn keys_page_after(
    prefix: &str,
    start_after_key: Option<&[u8]>,
    limit: u32,
) -> Result<Vec<Vec<u8>>> {
    let mut cap: usize = 256;
    let start_after = start_after_key.unwrap_or_default();

    loop {
        let mut out = vec![0u8; cap];
        let n = unsafe {
            ffi::db_keys_after(
                prefix.as_ptr() as u32,
                prefix.len() as u32,
                start_after.as_ptr() as u32,
                start_after.len() as u32,
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
                return parse_keys(&out);
            }
        }
    }
}

/// `scan(prefix) -> Result<Vec<(key, value)>, NxError>`
#[must_use = "this SDK call can fail; handle the Result"]
pub fn scan(prefix: &str) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    let mut last_key: Option<Vec<u8>> = None;
    let mut out = Vec::new();

    loop {
        let page = scan_page_after(prefix, last_key.as_deref(), DEFAULT_SCAN_LIMIT)?;
        if page.is_empty() {
            return Ok(out);
        }

        let is_last = page.len() < DEFAULT_SCAN_LIMIT as usize;
        last_key = page.last().map(|(key, _)| key.clone());
        out.extend(page);

        if is_last {
            return Ok(out);
        }
    }
}

/// `keys(prefix) -> Result<Vec<key>, NxError>`
#[must_use = "this SDK call can fail; handle the Result"]
pub fn keys(prefix: &str) -> Result<Vec<Vec<u8>>> {
    let mut last_key: Option<Vec<u8>> = None;
    let mut out = Vec::new();

    loop {
        let page = keys_page_after(prefix, last_key.as_deref(), DEFAULT_SCAN_LIMIT)?;
        if page.is_empty() {
            return Ok(out);
        }

        let is_last = page.len() < DEFAULT_SCAN_LIMIT as usize;
        last_key = page.last().cloned();
        out.extend(page);

        if is_last {
            return Ok(out);
        }
    }
}

// get(key) -> Result<Option<Vec<u8>>, NxError>
#[must_use = "this SDK call can fail; handle the Result"]
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

#[cfg(test)]
mod tests {
    use super::*;

    fn u32_le(n: u32) -> Vec<u8> {
        n.to_le_bytes().to_vec()
    }

    /// Builds a scan payload: `count`, then for each row `key_len`, `value_len`, key, value.
    fn scan_payload(rows: &[(&[u8], &[u8])]) -> Vec<u8> {
        let mut buf = u32_le(rows.len() as u32);
        for (key, value) in rows {
            buf.extend(u32_le(key.len() as u32));
            buf.extend(u32_le(value.len() as u32));
            buf.extend_from_slice(key);
            buf.extend_from_slice(value);
        }
        buf
    }

    /// Builds a keys payload: `count`, then for each key `key_len`, key.
    fn keys_payload(keys: &[&[u8]]) -> Vec<u8> {
        let mut buf = u32_le(keys.len() as u32);
        for key in keys {
            buf.extend(u32_le(key.len() as u32));
            buf.extend_from_slice(key);
        }
        buf
    }

    #[test]
    fn read_u32_le_reads_and_advances_offset() {
        let buf = [1, 0, 0, 0, 2, 1, 0, 0];
        let mut offset = 0;
        assert_eq!(read_u32_le(&buf, &mut offset), Ok(1));
        assert_eq!(offset, 4);
        assert_eq!(read_u32_le(&buf, &mut offset), Ok(258));
        assert_eq!(offset, 8);
    }

    #[test]
    fn read_u32_le_rejects_short_buffer() {
        let mut offset = 2;
        assert_eq!(
            read_u32_le(&[0, 0, 0, 0], &mut offset),
            Err(NxError::Internal)
        );
        assert_eq!(offset, 2);
    }

    #[test]
    fn parse_scan_rows_decodes_rows() {
        let buf = scan_payload(&[(b"a", b"1"), (b"bb", b""), (b"", b"xyz")]);
        let rows = parse_scan_rows(&buf).unwrap();
        assert_eq!(
            rows,
            vec![
                (b"a".to_vec(), b"1".to_vec()),
                (b"bb".to_vec(), Vec::new()),
                (Vec::new(), b"xyz".to_vec()),
            ]
        );
    }

    #[test]
    fn parse_scan_rows_accepts_empty_payload() {
        assert_eq!(parse_scan_rows(&u32_le(0)), Ok(Vec::new()));
    }

    #[test]
    fn parse_scan_rows_rejects_missing_count() {
        assert_eq!(parse_scan_rows(&[]), Err(NxError::Internal));
        assert_eq!(parse_scan_rows(&[1, 0]), Err(NxError::Internal));
    }

    #[test]
    fn parse_scan_rows_rejects_truncated_payload() {
        let buf = scan_payload(&[(b"key", b"value")]);
        // Cut inside the value, inside the key, and inside the length header.
        for cut in [buf.len() - 1, buf.len() - 6, 10] {
            assert_eq!(
                parse_scan_rows(&buf[..cut]),
                Err(NxError::Internal),
                "cut at {cut}"
            );
        }
    }

    #[test]
    fn parse_scan_rows_rejects_count_larger_than_rows() {
        let mut buf = scan_payload(&[(b"k", b"v")]);
        buf[..4].copy_from_slice(&u32_le(2));
        assert_eq!(parse_scan_rows(&buf), Err(NxError::Internal));
    }

    #[test]
    fn parse_scan_rows_rejects_oversized_lengths() {
        let mut buf = u32_le(1);
        buf.extend(u32_le(u32::MAX));
        buf.extend(u32_le(u32::MAX));
        buf.extend_from_slice(b"kv");
        assert_eq!(parse_scan_rows(&buf), Err(NxError::Internal));
    }

    #[test]
    fn parse_scan_rows_rejects_trailing_bytes() {
        let mut buf = scan_payload(&[(b"k", b"v")]);
        buf.push(0);
        assert_eq!(parse_scan_rows(&buf), Err(NxError::Internal));
    }

    #[test]
    fn parse_keys_decodes_keys() {
        let buf = keys_payload(&[b"a", b"", b"ccc"]);
        assert_eq!(
            parse_keys(&buf),
            Ok(vec![b"a".to_vec(), Vec::new(), b"ccc".to_vec()])
        );
    }

    #[test]
    fn parse_keys_accepts_empty_payload() {
        assert_eq!(parse_keys(&u32_le(0)), Ok(Vec::new()));
    }

    #[test]
    fn parse_keys_rejects_missing_count() {
        assert_eq!(parse_keys(&[]), Err(NxError::Internal));
        assert_eq!(parse_keys(&[1, 0, 0]), Err(NxError::Internal));
    }

    #[test]
    fn parse_keys_rejects_truncated_payload() {
        let buf = keys_payload(&[b"key"]);
        // Cut inside the key and inside the length header.
        for cut in [buf.len() - 1, 6] {
            assert_eq!(
                parse_keys(&buf[..cut]),
                Err(NxError::Internal),
                "cut at {cut}"
            );
        }
    }

    #[test]
    fn parse_keys_rejects_count_larger_than_keys() {
        let mut buf = keys_payload(&[b"k"]);
        buf[..4].copy_from_slice(&u32_le(2));
        assert_eq!(parse_keys(&buf), Err(NxError::Internal));
    }

    #[test]
    fn parse_keys_rejects_oversized_length() {
        let mut buf = u32_le(1);
        buf.extend(u32_le(u32::MAX));
        buf.extend_from_slice(b"k");
        assert_eq!(parse_keys(&buf), Err(NxError::Internal));
    }

    #[test]
    fn parse_keys_rejects_trailing_bytes() {
        let mut buf = keys_payload(&[b"k"]);
        buf.push(0);
        assert_eq!(parse_keys(&buf), Err(NxError::Internal));
    }
}
