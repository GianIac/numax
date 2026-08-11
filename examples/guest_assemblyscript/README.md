# AssemblyScript guest

This example calls the raw Numax WebAssembly ABI from AssemblyScript. It exports
`run`, imports `nx.host_log_v2`, and logs `Hello from AssemblyScript!`.

The example was tested with Node.js 22 and AssemblyScript 0.28.8. The compiler is
pinned in `package-lock.json`, so no global AssemblyScript installation is needed.

## Build and test

From this directory:

```sh
npm ci
npm run build
npm test
```

The build produces `guest.wasm` and a readable `guest.wat`. The smoke test checks
the module's imports and exports, instantiates it with a small JavaScript host, and
asserts the exact logged message.

## Run with Numax

Build the CLI from the repository root:

```sh
cargo build --release -p nx-cli
```

Then return to this directory and run:

```sh
../../target/release/nx run guest.wasm
```

The output includes:

```text
Hello from AssemblyScript!
```

## ABI notes

Numax logging uses a pointer and byte length into the guest's exported linear
memory. The message is therefore stored as static UTF-8 bytes with
`memory.data<u8>` and passed directly to `host_log_v2`.

Using `String.UTF8.encode` would allocate managed AssemblyScript memory and pull
in the standard `env.abort` import for allocation checks. Numax does not provide
that namespace, so this deliberately minimal guest uses static data and imports
only `nx.host_log_v2`.

This is a raw ABI example, not a generated AssemblyScript binding. The resulting
module does not depend on WASI; Node.js and npm are only build and test tools.
