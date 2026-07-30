# Time Clock Example

A tiny Numax guest showing the difference between wall-clock time and monotonic time.

- `time::now()` returns the current Unix timestamp in milliseconds.
- `time::monotonic()` returns milliseconds from a monotonic clock and is the right choice for measuring elapsed durations.

## Build

Install the bare WebAssembly target once if needed:

```bash
rustup target add wasm32-unknown-unknown
```

Then build the guest:

```bash
cd examples/time_clock
cargo build --release --target wasm32-unknown-unknown
```

## Run

From `examples/time_clock`:

```bash
nx run target/wasm32-unknown-unknown/release/time_clock.wasm
```

The output includes values like:

```text
time_clock: unix_time_ms=1785436000000
time_clock: elapsed_ms=1
```

The example uses `now()` for an absolute timestamp and brackets a small, side-effect-free CPU loop with `monotonic()`. A very fast machine may report `elapsed_ms=0` because the host clock has millisecond resolution.

Use monotonic time for durations because wall-clock time can jump when the system clock is adjusted.
