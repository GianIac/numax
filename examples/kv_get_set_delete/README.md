# KV Get Set Delete example

Demonstrates the complete lifecycle of a key/value entry using the Numax SDK.

It calls `db::set()`, `db::get()`, `db::exists()` and `db::delete("user:1")` Operations performed:

1. set `db::set("user:1", b"alice")`
2. get `db::get("user:1")`
3. exists `db::exists("user:1")`
4. delete `db::delete("user:1")`
5. exists `db::exists("user:1")`

> This example uses the high-level `nx-sdk` APIs and avoids direct host FFI calls, making it a good reference after `hello_sdk`.

## Build

```bash
cargo build --release --target wasm32-unknown-unknown
```

## Run

Using the local `nx` binary:

```bash
nx run target/wasm32-unknown-unknown/release/kv_get_set_delete.wasm
```

or if you want to run the runtime executable manually:

```bash
.\target\release\nx.exe run .\examples\kv_get_set_delete\target\wasm32-unknown-unknown\release\kv_get_set_delete.wasm
```

Expected Output:

```bash
[guest] kv_get_set_delete: start
[guest] Setting key 'user:1'
[guest] Set successful
[guest] Getting key 'user:1'
[guest] Value: alice
[guest] Checking for 'user:1' existence
[guest] Exists: true
[guest] Deleting Key 'user:1'
[guest] Delete successful
[guest] Checking for 'user:1' existence
[guest] Exists after delete: false
[guest] kv_get_set_delete: done
```

## Notes

- Build the example using the `wasm32-unknown-unknown` target.
- The example is intended to be executed with `nx run`.
- Values are stored as raw bytes (`&[u8]`) and converted to UTF-8 for logging. (see line 19 in `src/lib.rs`)
