use anyhow::Result;
use nx_sync::NodeId;
use wasmtime::{Caller, Linker, Memory};

use crate::runtime::HostState;

const ERR_BUF_TOO_SMALL: i32 = -2;
const ERR_INTERNAL: i32 = -3;
const ERR_SYNC_DISABLED: i32 = -5;

const MAX_OUT_CAP: u32 = 1024 * 1024;

fn get_memory(caller: &mut Caller<'_, HostState>) -> Option<Memory> {
    match caller.get_export("memory") {
        Some(wasmtime::Extern::Memory(mem)) => Some(mem),
        _ => None,
    }
}

fn write_bytes(
    caller: &mut Caller<'_, HostState>,
    memory: &Memory,
    out_ptr: u32,
    out_cap: u32,
    bytes: &[u8],
    api_name: &str,
) -> i32 {
    if out_cap > MAX_OUT_CAP {
        eprintln!("[nx-core] {api_name}: output capacity too large: {out_cap} (max {MAX_OUT_CAP})");
        return ERR_INTERNAL;
    }
    if bytes.len() > out_cap as usize {
        return ERR_BUF_TOO_SMALL;
    }
    if let Err(e) = memory.write(caller, out_ptr as usize, bytes) {
        eprintln!("[nx-core] {api_name}: failed to write output: {e}");
        return ERR_INTERNAL;
    }

    bytes.len() as i32
}

fn encode_peers(peers: &[(String, NodeId)]) -> std::result::Result<Vec<u8>, i32> {
    let peer_count = u32::try_from(peers.len()).map_err(|_| ERR_INTERNAL)?;
    let mut out = Vec::new();
    out.extend_from_slice(&peer_count.to_le_bytes());

    for (addr, node_id) in peers {
        let addr = addr.as_bytes();
        let node_id = node_id.as_str().as_bytes();
        let addr_len = u32::try_from(addr.len()).map_err(|_| ERR_INTERNAL)?;
        let node_id_len = u32::try_from(node_id.len()).map_err(|_| ERR_INTERNAL)?;

        out.extend_from_slice(&addr_len.to_le_bytes());
        out.extend_from_slice(&node_id_len.to_le_bytes());
        out.extend_from_slice(addr);
        out.extend_from_slice(node_id);
    }

    Ok(out)
}

fn net_node_id_impl(mut caller: Caller<'_, HostState>, out_ptr: u32, out_cap: u32) -> i32 {
    let memory = match get_memory(&mut caller) {
        Some(m) => m,
        None => {
            eprintln!("[nx-core] net_node_id: no `memory` export on guest");
            return ERR_INTERNAL;
        }
    };
    let Some(handle) = caller.data().sync_handle.as_ref() else {
        return ERR_SYNC_DISABLED;
    };
    let node_id = handle.node_id().as_str().as_bytes().to_vec();

    write_bytes(
        &mut caller,
        &memory,
        out_ptr,
        out_cap,
        &node_id,
        "net_node_id",
    )
}

async fn net_peers_impl(mut caller: Caller<'_, HostState>, out_ptr: u32, out_cap: u32) -> i32 {
    let memory = match get_memory(&mut caller) {
        Some(m) => m,
        None => {
            eprintln!("[nx-core] net_peers: no `memory` export on guest");
            return ERR_INTERNAL;
        }
    };
    let Some(handle) = caller.data().sync_handle.as_ref().cloned() else {
        return ERR_SYNC_DISABLED;
    };

    let peers = handle.connected_peers().await;
    let encoded = match encode_peers(&peers) {
        Ok(encoded) => encoded,
        Err(code) => return code,
    };

    write_bytes(
        &mut caller,
        &memory,
        out_ptr,
        out_cap,
        &encoded,
        "net_peers",
    )
}

pub fn add_to_linker(linker: &mut Linker<HostState>) -> Result<()> {
    linker.func_wrap(
        "nx",
        "net_node_id",
        |caller: Caller<'_, HostState>, out_ptr: u32, out_cap: u32| -> i32 {
            net_node_id_impl(caller, out_ptr, out_cap)
        },
    )?;

    linker.func_wrap_async(
        "nx",
        "net_peers",
        |caller: Caller<'_, HostState>, (out_ptr, out_cap): (u32, u32)| {
            Box::new(net_peers_impl(caller, out_ptr, out_cap))
        },
    )?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_peers_includes_addr_and_node_id() {
        let bytes = encode_peers(&[("127.0.0.1:9000".to_string(), NodeId::new("node-a"))]).unwrap();

        assert_eq!(u32::from_le_bytes(bytes[0..4].try_into().unwrap()), 1);
        assert!(bytes.ends_with(b"127.0.0.1:9000node-a"));
    }
}
