#!/usr/bin/env bash

set -e

if ! command -v tinygo >/dev/null 2>&1; then
    echo "[error] tinygo not found in PATH"
    exit 1
fi

echo "Building guest_tinygo WASM module"

tinygo build -opt=0 -o guest.wasm -target wasi -no-debug src/main.go

echo
echo "Build complete:"
echo "guest.wasm"