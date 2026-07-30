# Crypto Hashing Example

A tiny Numax guest demonstrating secure random bytes, SHA-256, and BLAKE3 through `nx_sdk::crypto`.

## Build

Install the bare WebAssembly target once if needed:

```bash
rustup target add wasm32-unknown-unknown
```

Then build the guest:

```bash
cd examples/crypto_hashing
cargo build --release --target wasm32-unknown-unknown
```

## Run

From `examples/crypto_hashing`:

```bash
nx run target/wasm32-unknown-unknown/release/crypto_hashing.wasm
```

The output has this shape:

```text
crypto_hashing: random=<32 lowercase hex characters>
crypto_hashing: sha256=<64 lowercase hex characters>
crypto_hashing: blake3=<64 lowercase hex characters>
```

The random value is 16 bytes and changes on each run. Both SHA-256 and BLAKE3 return 32-byte digests, displayed here as 64 hexadecimal characters.

The example encodes bytes directly and does not need an additional hex dependency.
