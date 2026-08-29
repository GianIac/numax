# Numax Examples

This directory contains runnable examples that show how to build guests for Numax,
from a minimal "hello world" to distributed CRDT-backed applications. Each example
has its own `README.md` with build and run instructions start with whichever
group matches what you're trying to learn.

## Hello world

Minimal guests that show the different ways to talk to the Numax host.

| Example | Description |
| --- | --- |
| [`hello_sdk`](hello_sdk/README.md) | Smallest Numax SDK example; calls `nx_sdk::log`. |
| [`hello_wasm`](hello_wasm/README.md) | Minimal raw WebAssembly guest that imports `nx.host_log` directly. |
| [`hello_wasi`](hello_wasi/README.md) | Minimal WASI module running on Numax, using stdout and WASI args. |

## Distributed / CRDT

Examples that replicate state across Numax nodes using CRDTs and converge through the sync layer.

| Example | Description |
| --- | --- |
| [`distributed_counter`](distributed_counter/README.md) | Grow-only distributed counter (GCounter). |
| [`distributed_inventory`](distributed_inventory/README.md) | Two-way stock counter (PNCounter) for restocks, sales, and returns. |
| [`distributed_ants`](distributed_ants/README.md) | Distributed Ant Colony Optimization swarm: a shared pheromone trail (PNCounter grid) emerges from many independent nodes. |
| [`distributed_status`](distributed_status/README.md) | Single latest-value service status (LWW-Register). |
| [`distributed_settings`](distributed_settings/README.md) | Distributed settings document with per-field last-writer-wins resolution (LWW-Map). |
| [`distributed_tags`](distributed_tags/README.md) | Distributed tag set with add/remove semantics (ORSet). |
| [`distributed_comments`](distributed_comments/README.md) | Ordered, replicated comment stream (RGA) with stable ids and tombstone deletes. |
| [`vote_tally_tls`](vote_tally_tls/README.md) | Three-node vote tally (GCounter) replicated over mTLS with an allowlist. |

## Local key/value

Examples that use Numax's local key-value store (`nx_sdk::db::*` or the raw host API), with no replication involved.

| Example | Description |
| --- | --- |
| [`kv_counter`](kv_counter/README.md) | Persistent local counter using the key-value host API through the SDK. |
| [`kv_get_set_delete`](kv_get_set_delete/README.md) | Full lifecycle of a key/value entry (`set`, `get`, `exists`, `delete`) via the SDK. |
| [`kv_roundtrip`](kv_roundtrip/README.md) | Raw host API roundtrip (`db_set`, `db_get`, `db_delete`) without the SDK. |
| [`kv_sdk_roundtrip`](kv_sdk_roundtrip/README.md) | SDK version of the local key-value roundtrip. |
| [`distributed_chat`](distributed_chat/README.md) | Local chat log built on the key-value API; intentionally not replicated. |

## Non-Rust guests

Examples that build guests in languages other than Rust.

| Example | Description |
| --- | --- |
| [`guest_c`](guest_c/README.md) | Minimal C guest compiled to WASM, with manual host imports and no libc. |
| [`guest_cpp`](guest_cpp/README.md) | Minimal C++ guest compiled to WASM, using `import_name`/`export_name` instead of `extern "C"`. |
