use crate::__alloc::string::String;
use crate::__alloc::vec;
use crate::__alloc::vec::Vec;

use crate::error::{NxError, Result};
use crate::ffi;

const ERR_BUF_TOO_SMALL: i32 = -2;
const ERR_INTERNAL: i32 = -3;
const ERR_SYNC_DISABLED: i32 = -5;
const MAX_NET_BUFFER: usize = 1024 * 1024;

/// Connected peer information returned by `net_peers`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Peer {
    pub addr: String,
    pub node_id: String,
}

fn read_dynamic(mut call: impl FnMut(*mut u8, u32) -> i32) -> Result<Vec<u8>> {
    let mut cap = 64usize;

    loop {
        let mut out = vec![0u8; cap];
        let rc = call(out.as_mut_ptr(), out.len() as u32);

        match rc {
            ERR_INTERNAL => return Err(NxError::Internal),
            ERR_SYNC_DISABLED => return Err(NxError::SyncDisabled),
            ERR_BUF_TOO_SMALL => {
                cap = cap.saturating_mul(2);
                if cap > MAX_NET_BUFFER {
                    return Err(NxError::BufferTooSmall);
                }
                continue;
            }
            c if c < 0 => return Err(NxError::UnknownCode(c)),
            n => {
                out.truncate(n as usize);
                return Ok(out);
            }
        }
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

fn parse_peers(buf: &[u8]) -> Result<Vec<Peer>> {
    let mut offset = 0usize;
    let count = read_u32_le(buf, &mut offset)? as usize;
    let mut peers = Vec::new();

    for _ in 0..count {
        let addr_len = read_u32_le(buf, &mut offset)? as usize;
        let node_id_len = read_u32_le(buf, &mut offset)? as usize;
        let addr_end = offset.saturating_add(addr_len);
        let node_id_end = addr_end.saturating_add(node_id_len);
        if addr_end > buf.len() || node_id_end > buf.len() {
            return Err(NxError::Internal);
        }

        let addr =
            String::from_utf8(buf[offset..addr_end].to_vec()).map_err(|_| NxError::Internal)?;
        let node_id = String::from_utf8(buf[addr_end..node_id_end].to_vec())
            .map_err(|_| NxError::Internal)?;
        peers.push(Peer { addr, node_id });
        offset = node_id_end;
    }

    if offset != buf.len() {
        return Err(NxError::Internal);
    }

    Ok(peers)
}

/// Local sync NodeId. Requires sync to be enabled.
pub fn node_id() -> Result<String> {
    let bytes =
        read_dynamic(|out_ptr, out_cap| unsafe { ffi::net_node_id(out_ptr as u32, out_cap) })?;
    String::from_utf8(bytes).map_err(|_| NxError::Internal)
}

/// Currently connected sync peers. Requires sync to be enabled.
pub fn peers() -> Result<Vec<Peer>> {
    let bytes =
        read_dynamic(|out_ptr, out_cap| unsafe { ffi::net_peers(out_ptr as u32, out_cap) })?;
    parse_peers(&bytes)
}
