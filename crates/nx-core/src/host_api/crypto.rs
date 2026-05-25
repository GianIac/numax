use anyhow::Result;
use sha2::{Digest, Sha256};
use wasmtime::{Caller, Linker, Memory};

use crate::runtime::HostState;

const ERR_BUF_TOO_SMALL: i32 = -2;
const ERR_INTERNAL: i32 = -3;

const HASH_LEN: usize = 32;
const MAX_CRYPTO_LEN: u32 = 1024 * 1024; // 1 MiB

fn get_memory(caller: &mut Caller<'_, HostState>) -> Option<Memory> {
    match caller.get_export("memory") {
        Some(wasmtime::Extern::Memory(mem)) => Some(mem),
        _ => None,
    }
}

fn read_bytes(
    caller: &mut Caller<'_, HostState>,
    memory: &Memory,
    ptr: u32,
    len: u32,
) -> Result<Vec<u8>> {
    if len > MAX_CRYPTO_LEN {
        anyhow::bail!("requested length too large: {len} > {MAX_CRYPTO_LEN}");
    }

    let mut buf = vec![0u8; len as usize];
    memory.read(caller, ptr as usize, &mut buf)?;
    Ok(buf)
}

fn sha256_digest(input: &[u8]) -> [u8; HASH_LEN] {
    Sha256::digest(input).into()
}

fn blake3_digest(input: &[u8]) -> [u8; HASH_LEN] {
    *blake3::hash(input).as_bytes()
}

fn random_bytes_impl(mut caller: Caller<'_, HostState>, out_ptr: u32, out_len: u32) -> i32 {
    let memory = match get_memory(&mut caller) {
        Some(m) => m,
        None => {
            eprintln!("[nx-core] random_bytes: no `memory` export on guest");
            return ERR_INTERNAL;
        }
    };

    if out_len > MAX_CRYPTO_LEN {
        eprintln!(
            "[nx-core] random_bytes: invalid output length: {out_len} (max {MAX_CRYPTO_LEN})"
        );
        return ERR_INTERNAL;
    }

    let mut out = vec![0u8; out_len as usize];
    if let Err(e) = getrandom::fill(&mut out) {
        eprintln!("[nx-core] random_bytes: entropy source error: {e}");
        return ERR_INTERNAL;
    }

    if let Err(e) = memory.write(&mut caller, out_ptr as usize, &out) {
        eprintln!("[nx-core] random_bytes: failed to write output: {e}");
        return ERR_INTERNAL;
    }

    out_len as i32
}

fn hash_impl(
    mut caller: Caller<'_, HostState>,
    input_ptr: u32,
    input_len: u32,
    out_ptr: u32,
    out_cap: u32,
    api_name: &str,
    digest: fn(&[u8]) -> [u8; HASH_LEN],
) -> i32 {
    let memory = match get_memory(&mut caller) {
        Some(m) => m,
        None => {
            eprintln!("[nx-core] {api_name}: no `memory` export on guest");
            return ERR_INTERNAL;
        }
    };

    if out_cap < HASH_LEN as u32 {
        return ERR_BUF_TOO_SMALL;
    }
    if out_cap > MAX_CRYPTO_LEN {
        eprintln!(
            "[nx-core] {api_name}: output capacity too large: {out_cap} (max {MAX_CRYPTO_LEN})"
        );
        return ERR_INTERNAL;
    }

    let input = match read_bytes(&mut caller, &memory, input_ptr, input_len) {
        Ok(input) => input,
        Err(e) => {
            eprintln!("[nx-core] {api_name}: failed to read input: {e}");
            return ERR_INTERNAL;
        }
    };
    let out = digest(&input);

    if let Err(e) = memory.write(&mut caller, out_ptr as usize, &out) {
        eprintln!("[nx-core] {api_name}: failed to write output: {e}");
        return ERR_INTERNAL;
    }

    HASH_LEN as i32
}

pub fn add_to_linker(linker: &mut Linker<HostState>) -> Result<()> {
    linker.func_wrap(
        "nx",
        "random_bytes",
        |caller: Caller<'_, HostState>, out_ptr: u32, out_len: u32| -> i32 {
            random_bytes_impl(caller, out_ptr, out_len)
        },
    )?;

    linker.func_wrap(
        "nx",
        "hash_sha256",
        |caller: Caller<'_, HostState>,
         input_ptr: u32,
         input_len: u32,
         out_ptr: u32,
         out_cap: u32|
         -> i32 {
            hash_impl(
                caller,
                input_ptr,
                input_len,
                out_ptr,
                out_cap,
                "hash_sha256",
                sha256_digest,
            )
        },
    )?;

    linker.func_wrap(
        "nx",
        "hash_blake3",
        |caller: Caller<'_, HostState>,
         input_ptr: u32,
         input_len: u32,
         out_ptr: u32,
         out_cap: u32|
         -> i32 {
            hash_impl(
                caller,
                input_ptr,
                input_len,
                out_ptr,
                out_cap,
                "hash_blake3",
                blake3_digest,
            )
        },
    )?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex(bytes: &[u8]) -> String {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut out = String::with_capacity(bytes.len() * 2);
        for byte in bytes {
            out.push(HEX[(byte >> 4) as usize] as char);
            out.push(HEX[(byte & 0x0f) as usize] as char);
        }
        out
    }

    #[test]
    fn sha256_digest_matches_known_vector() {
        assert_eq!(
            hex(&sha256_digest(b"abc")),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[test]
    fn blake3_digest_matches_known_vector() {
        assert_eq!(
            hex(&blake3_digest(b"abc")),
            "6437b3ac38465133ffb63b75273a8db548c558465d79db03fd359c6cd5bd9d85"
        );
    }
}
