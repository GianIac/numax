#!/usr/bin/env sh
set -eu

cd "$(dirname "$0")"

zig build-exe src/main.zig \
  -target wasm32-freestanding \
  -O ReleaseSmall \
  -fno-entry \
  --export-memory \
  --export=run \
  -femit-bin=guest.wasm
