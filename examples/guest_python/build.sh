#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

rustup target add wasm32-wasip1 >/dev/null 2>&1 || true

# --export=run: rustc/wasm-ld won't export arbitrary #[no_mangle] fns
# from a "bin" crate by default, only what _start needs. This forces it.
RUSTFLAGS="-C link-arg=--export=run" \
  cargo build --release --target wasm32-wasip1

cp target/wasm32-wasip1/release/guest.wasm guest.wasm
echo "Built examples/guest_python/guest.wasm"