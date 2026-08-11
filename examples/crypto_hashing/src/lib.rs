use nx_sdk::{crypto, nx_log};

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";

    let mut encoded = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        encoded.push(DIGITS[(byte >> 4) as usize] as char);
        encoded.push(DIGITS[(byte & 0x0f) as usize] as char);
    }
    encoded
}

#[unsafe(no_mangle)]
pub extern "C" fn run() {
    let random = crypto::random_bytes(16).unwrap();
    let sha256 = crypto::hash_sha256(b"hello numax").unwrap();
    let blake3 = crypto::hash_blake3(b"hello numax").unwrap();

    nx_log!("crypto_hashing: random={}", hex(&random));
    nx_log!("crypto_hashing: sha256={}", hex(&sha256));
    nx_log!("crypto_hashing: blake3={}", hex(&blake3));
}
