---
title: nx-store
description: Embedded key/value store abstraction.
---

`nx-store` is a thin, typed wrapper over [sled](https://github.com/spacejam/sled),
a pure-Rust embedded key/value database. It is the only crate in the workspace
that touches persistent storage directly. Everything else that needs durability
goes through it.

It has no async, no networking, no CRDT logic, no dependencies on the rest of the workspace.
It is opened once in `nx-core::Runtime::new` and shared via `Arc<Store>` with the sync manager
and every `HostState` produced by `run_module`.

---

## Responsibilities

| Responsibility | Where |
|---|---|
| Open or create the sled database | `store.rs` `Store::open` |
| Single-key read, write, delete, exists | `store.rs` basic ops |
| Atomic multi-key batch (sets + deletes) | `store.rs` `Store::apply_batch` |
| Prefix scan with offset cursor | `store.rs` `scan_prefix_page` |
| Prefix scan with key cursor (preferred) | `store.rs` `scan_prefix_page_after` |
| Keys-only listing with offset cursor | `store.rs` `keys_prefix_page` |
| Keys-only listing with key cursor (preferred) | `store.rs` `keys_prefix_page_after` |
| Explicit flush to disk | `store.rs` `Store::flush` |
| Store statistics (key count, total bytes) | `store.rs` `Store::stats` |
| Error types | `error.rs` `StoreError` |

---

## Store

`Store` is the single public struct. It wraps `sled::Db` and derives `Clone`
because sled handles are cheap to clone (they reference-count the underlying database).

```rust
pub struct Store {
    db: sled::Db,  // Clone is cheap: sled::Db is an Arc internally
}
```

### Opening

```rust
let store = Store::open("./nx-data")?;
```

`Store::open` creates the directory with `fs::create_dir_all` if it does not exist.
If the path exists but is not a directory, it returns `StoreError::NotADirectory`.

The store directory is owned by a single node. Each node must use its own path.

### Basic operations

```rust
// write
store.set(b"user:1", b"alice")?;

// read
let val: Option<Vec<u8>> = store.get(b"user:1")?;

// check existence
let found: bool = store.exists(b"user:1")?;

// delete (no-op if key does not exist)
store.delete(b"user:1")?;

// explicit flush (called on shutdown)
store.flush()?;
```

`set` does not flush to disk immediately. sled provides crash safety via its own
write-ahead log, but the explicit `flush` call in `Runtime::shutdown_inner` ensures
all writes are durable before the process exits.

### Atomic batch

```rust
store.apply_batch(
    &[(b"new_key", b"new_value")],  // sets
    &[b"old_key"],                  // deletes
)?;
```

`apply_batch` wraps sets and deletes into a single `sled::Batch` and applies it atomically.
Used by the sync manager to persist CRDT state and op-log entries together.

### Stats

```rust
let stats: StoreStats = store.stats()?;
// stats.keys  -> total number of keys
// stats.bytes -> total bytes (key + value lengths, summed)
```

`stats()` iterates the full database. It is used by the observability endpoint.
Do not call it in a hot path.

---

## Prefix scan

The store exposes four scan variants. Two include values, two return keys only.
Each pair has an offset-cursor version (for compatibility) and a key-cursor version (preferred).

### Why key cursors are preferred

Offset cursors count visible rows from position 0 on every call. If a key is inserted
before the current offset between two paginated calls, the offset shifts and a row
can be returned twice or skipped entirely.

Key cursors use `db.range(start_after..)` and skip until the key is strictly greater
than the cursor. Insertions before the cursor do not affect the result.

### scan_prefix_page (offset cursor)

```rust
let page: Vec<(Vec<u8>, Vec<u8>)> = store.scan_prefix_page(
    b"app:",         // prefix
    0,               // offset cursor (row index)
    64,              // page size
    Some(b"__nx/"),  // excluded prefix (pass None if not needed)
)?;
```

### scan_prefix_page_after (key cursor, preferred)

```rust
// first page
let page = store.scan_prefix_page_after(b"app:", None, 64, None)?;

// next page: pass last key from previous page
let last_key = page.last().map(|(k, _)| k.clone());
let page = store.scan_prefix_page_after(b"app:", last_key.as_deref(), 64, None)?;
```

### keys_prefix_page (offset cursor)

```rust
let keys: Vec<Vec<u8>> = store.keys_prefix_page(b"app:", 0, 64, None)?;
```

### keys_prefix_page_after (key cursor, preferred)

```rust
let keys = store.keys_prefix_page_after(b"app:", None, 64, None)?;
let keys = store.keys_prefix_page_after(b"app:", last_key.as_deref(), 64, None)?;
```

### excluded_prefix

All four scan methods accept an `excluded_prefix: Option<&[u8]>`.
The host API layer passes `Some(b"__nx/")` to hide runtime-reserved keys from guest code.
Pass `None` when scanning internal keys from within `nx-core`.

---

## Reserved key prefix

Keys under `__nx/` are used by the runtime for its own state (NodeId, CRDT persistence, op-log).
The host API in `nx-core` passes `excluded_prefix = Some(b"__nx/")` to all scan calls
so guest modules never see them, and rejects direct `db_get`/`db_set`/`db_delete` calls
on reserved keys with error code `-4` (`ERR_RESERVED_KEY`).

`nx-store` itself does not enforce this rule. Enforcement is in `nx-core/src/host_api/db.rs`.

---

## Error types

```rust
pub enum StoreError {
    Sled(sled::Error),
    Io(std::io::Error),
    NotADirectory(String),
}
```

`Sled` wraps any error from the sled engine.
`Io` wraps filesystem errors from directory creation.
`NotADirectory` is returned when the path exists but is a file.

---

## Test coverage

Tests live in `lib.rs` (`#[cfg(test)]`), plus integration tests in `tests/`.
All tests use `tempfile::tempdir()` for isolation.

| Test | What it covers |
|---|---|
| `test_set_and_get` | basic write/read roundtrip |
| `test_get_nonexistent` | missing key returns None |
| `test_exists` | exists() before set, after set, after delete |
| `test_overwrite` | second set replaces first value |
| `test_delete` | delete makes key return None |
| `test_multiple_keys` | independent keys do not interfere |
| `test_apply_batch_sets_and_deletes_atomically` | batch sets new key and removes old key |
| `test_scan_prefix_page_paginates_visible_keys` | offset cursor pages correctly |
| `test_scan_prefix_page_excludes_reserved_prefix` | `__nx/` key is hidden when excluded |
| `test_scan_prefix_page_after_uses_key_cursor` | key cursor pages correctly |
| `test_scan_prefix_page_after_does_not_shift_when_key_is_inserted_before_cursor` | key cursor is stable against insertions |
| `test_keys_prefix_page_paginates_visible_keys` | offset cursor keys-only |
| `test_keys_prefix_page_excludes_reserved_prefix` | `__nx/` key hidden in keys-only scan |
| `test_keys_prefix_page_after_uses_key_cursor` | key cursor keys-only |
| `test_stats_counts_keys_and_bytes` | stats counts keys and byte lengths correctly |

```bash
cargo test -p nx-store
```

---

## Related

Use this page together with the runtime and user-facing storage docs:

- [Crates overview](/numax/reference/crates/) - where `nx-store` fits in the dependency graph
- [nx-core crate](/numax/reference/crates/nx-core/) - opens and shares the `Store`
- [Host API](/numax/reference/host-api/) - `db_*` functions that call into the store through `nx-core`
- [Configuration](/numax/reference/configuration/) - `[storage].datastore_path` that becomes the store path
