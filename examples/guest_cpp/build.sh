#!/usr/bin/env bash

set -e

if ! command -v clang++ >/dev/null 2>&1; then
    echo "[error] clang++ not found in PATH"
    exit 1
fi

echo "Building guest_cpp WASM module"

clang++ \
  --target=wasm32-wasip1 \
  -O3 \
  -nostdlib \
  -Wl,--no-entry \
  -Wl,--export=run \
  -Wl,--allow-undefined \
  -o guest.wasm \
  src/guest.cpp

echo
echo "Build complete:"
echo "guest.wasm"
