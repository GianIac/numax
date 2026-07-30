# Zig guest

This minimal guest uses Zig 0.16.0 to build a freestanding core WebAssembly
module. It imports `host_log_v2` directly from the `nx` namespace and exports
the `run` entrypoint Numax expects.

## Requirements

- Zig 0.16.0
- Node.js 22 or newer for the ABI smoke test
- a built Numax CLI for the runtime smoke test

## Build and test

From the repository root:

```sh
examples/guest_zig/build.sh
node examples/guest_zig/test.mjs
```

The build writes `examples/guest_zig/guest.wasm`. The test verifies that the
module imports only `nx.host_log_v2`, exports `memory` and `run`, and logs the
expected message through a mock host.

## Run with Numax

```sh
cargo build -p nx-cli
./target/debug/nx run examples/guest_zig/guest.wasm
```

Expected output includes:

```text
[guest] Hello from Zig guest!
```

## ABI notes and limitations

- This is a core WebAssembly module, not a Component Model component.
- Zig calls the raw Numax ABI directly; there is no generated Zig SDK yet.
- Strings cross the boundary as a pointer and byte length into exported guest
  memory.
- The example logs one static message and does not demonstrate allocation,
  database access, or error recovery.
