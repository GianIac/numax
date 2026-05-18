# Numax Roadmap

> **Current release**: `v0.1.0-alpha.3` - developer preview.
> **Final goal `v0.1.0`**: production-ready runtime for non-critical workloads.
> **Status**: alpha for feedback; production hardening still in progress.

---

## Release Status

### v0.1.0-alpha.2 ✅
**Purpose**: second technical preview, focused on making sync usable from the CLI.

Includes:
- Wasmtime runtime + WASI preview1.
- Local KV host API (`db_get`, `db_set`, `db_delete`) and logging.
- Embedded sled store.
- GCounter CRDT with JSON serialization.
- Base networking with Hello/PushOps/PullSince/Ping.
- TLS/mTLS, NodeID derived from certificate and allowlist.
- End-to-end internal wiring between guest CRDT API, SyncManager and datastore.
- Sled materialization of the GCounter total on local/remote update.
- Runtime lifecycle for sync-enabled nodes: long-running mode, settle mode,
  signal-aware shutdown and final store flush.
- Startup hydration of materialized GCounter values.
- `SyncManager` E2E tests for handshake, PushOps, convergence and idempotency.
- Examples: `distributed_counter`, `distributed_chat` local-only, `vote_tally_tls`.

Known limitations:
- Full network resilience, serialization hardening and the extended host API are
  still open phases.
- API and wire format may change before `v0.1.0`.

### v0.1.0-alpha.3 ✅
**Purpose**: hardening preview for lifecycle, backpressure and observability.

Includes:
- Everything in `v0.1.0-alpha.2`.
- Phase 7 lifecycle hardening: stable runtime NodeId, startup hydration,
  cooperative network read-loop shutdown, bounded task shutdown fallback and
  final store flush.
- Phase 8 backpressure and limits: peer limit during handshake, queued ops
  limit, message size limit and socket read/write timeouts.
- Phase 9 minimal observability: configurable logging, `/metrics`, `/health`
  and `/ready`.

Known limitations:
- Durable full CRDT state/op-log recovery, automatic reconnect and anti-entropy
  remain tracked in Phase 10.
- API and wire format may change before `v0.1.0`.

### v0.1.0 🎯
**Purpose**: first production-ready release for non-critical workloads.

Requires the completion of the P0/P1 phases listed below, in particular:
Phase 7 lifecycle, Phase 8 backpressure, Phase 9 minimal observability,
Phase 10 network resilience, Phase 11 dual-mode serialization, Phase 12 minimal
host API and Phase 13 load testing.

---

## Completed Phases

### Phase 0: Bootstrap ✅
- [x] Multi-crate Cargo workspace
- [x] Directory structure
- [x] Base CI

### Phase 1: nx-core ✅
- [x] Wasmtime runtime
- [x] Host API (db_get, db_set, db_delete, host_log_v2)
- [x] WASI preview1 integration
- [x] Security guardrails (key/value limits)

### Phase 2: nx-store ✅
- [x] Embedded sled store
- [x] get/set/delete/scan_prefix API
- [x] Unit and integration tests

### Phase 3: nx-sync ✅
- [x] NodeId and Op/OpId
- [x] Complete GCounter CRDT
- [x] CRDT property tests (commutativity, associativity, idempotency)
- [x] JSON serialization

### Phase 4: nx-net ✅
- [x] Message protocol (Hello, PushOps, PullSince, Ping/Pong)
- [x] Length-prefixed framing
- [x] Protocol versioning

### Phase 5: Documentation and CI ✅
- [x] Automated tests
- [x] Multi-OS CI (Ubuntu, macOS, Windows)
- [x] Clippy + rustfmt
- [x] WHITEPAPER.md aligned with the code
- [x] HOST_API.md
- [x] WASM examples (distributed_counter, distributed_chat)

> ⚠️ Note: the Phase 5 examples worked only locally; end-to-end convergence
> between peers was actually wired up in Phase 6.5.

---

## Production-Ready Phases

### Phase 6: Transport Security 🔒 ✅
**Goal**: Secure and authenticated communications between nodes.

**Base TLS:**
- [x] TLS 1.3 for TCP connections
- [x] Auto-generated certificates for development (`rcgen`)
- [x] Custom certificates support for production
- [x] Forward secrecy (ECDHE automatic with TLS 1.3)
- [x] TLS wrapper: `TlsAcceptor` (server), `TlsConnector` (client)

**Mutual TLS (mTLS):**
- [x] Client must present a certificate
- [x] Server verifies the client certificate
- [x] Custom CA support for verification (`--tls-ca`)
- [x] Test: client without cert → rejected
- [x] Test: client with invalid cert → rejected

**Identity & NodeID:**
- [x] NodeID derived from public key: `NodeId = hash(cert.public_key)` (Protocol identity: 16 bytes and Fingerprint/debug: 32 bytes)
- [x] Function `derive_node_id_from_cert(cert) -> NodeId`
- [x] Verification during Hello handshake: cert.pubkey → expected NodeId
- [x] NodeID mismatch → immediate disconnect

**Peer Verification:**
- [x] Hostname/CN verification in the certificate
- [x] Optional allowlist of authorized NodeIDs
- [x] Connection from a NodeID not in the list → rejected (if allowlist is active)

**CLI Flags:**
- [x] `--tls-cert <path>` - Node certificate
- [x] `--tls-key <path>` - Node private key
- [x] `--tls-ca <path>` - CA used to verify peers
- [x] `--allowed-peers <id1,id2,...>` - NodeID allowlist
- [x] `--tls-insecure` - Dev only, skip verify (warning)

**Security Tests:**
- [x] Test: TLS connection works between 2 nodes
- [x] Test: connection rejected without certificate
- [x] Test: connection rejected with expired/invalid cert
- [x] Test: mTLS - both peers authenticated
- [x] Test: NodeID mismatch → disconnect
- [x] Test: peer not in allowlist → rejected
- [x] Test: tests for the new CLI flags

**Libraries**: `rustls`, `tokio-rustls`, `rcgen`, `sha2`

**Achieved security matrix:**

| Attack | Protected |
|--------|-----------|
| Eavesdropping | ✅ TLS |
| Tampering | ✅ TLS |
| Replay | ✅ TLS |
| MITM server | ✅ Cert verify |
| MITM client | ✅ mTLS |
| Rogue node | ✅ Allowlist |
| Spoofed NodeID | ✅ hash(pubkey) |

---

### Phase 6.5: End-to-End Sync Wiring 🔗
**Goal**: close the hidden gaps between guest WASM, SyncManager and datastore, so that replicable operations actually make the full round trip between peers.
Includes the restructuring of the host API to separate local KV and replicated CRDT without per-key magic.

**Async runtime**:
- [x] `Runtime::run_module` becomes `async` and runs inside a `tokio::Runtime`.
- [x] CLI switches to `#[tokio::main]`; `real_main` becomes async.
- [x] `SyncManager` accessible as `Arc<Mutex<SyncManager>>` (or cloneable handle).
- [x] `Runtime::start_sync` actually calls `SyncManager::start().await`.
- [x] `wasmtime` loaded with `add_to_linker_async` and `run.call_async` so it does
      not block the tokio runtime during host calls.

**CRDT Host API (new)**:
- [x] `crdt_gcounter_inc(key_ptr, key_len, delta: u64) -> i32`
      applies locally, materializes the total in sled and emits an Op via channel.
- [x] `crdt_gcounter_value(key_ptr, key_len, out_ptr, out_cap) -> i32`
      reads the current total from the in-memory registry.
- [x] SDK wrapper `nx_sdk::crdt::gcounter::{inc, value}`.

**End-to-end wiring**:
- [x] `HostState` includes a handle to the SyncManager (Op sender + GCounter accessor).
- [x] `apply_remote_op` updates the GCounter **and** rewrites the total in sled.
- [x] Atomic materialization: GCounter update → sled write in a single logical
      transaction (a sled batch is acceptable).

**Cleanup of the past**:
- [x] Remove the `--sync-prefix` CLI flag.
- [x] Update log messages and help.
- [x] Update `HOST_API.md` with the `db_*` vs `crdt_*` separation.

**Examples migration**:
- [x] `examples/distributed_counter`: rewritten with `nx_sdk::crdt::gcounter`.
- [x] `examples/distributed_chat`: marked as "non-replicated (local LWW)"
      or removed until ORSet/RGA are available (Phase 14).
- [x] `examples/vote_tally_tls`: new example with mTLS + allowlist + real
      CRDT counter across 3 nodes.

**Tests**:
- [x] E2E test: 2 nodes, A runs `gcounter::inc("visits", 1)`, after handshake
      and one round of PushOps B reads `gcounter::value("visits") == 1`.
- [x] E2E test: 2 nodes, A and B increment in parallel, converge on the same
      total.
- [x] Test: no Op emitted when sync is disabled.
- [x] Test: `apply_remote_op` is idempotent (same Op twice → no double counting).

**Closing criterion**:
```bash
# Terminal A
nx run counter.wasm --listen 127.0.0.1:9000 --peer 127.0.0.1:9001 \
    --datastore-path ./data-a --wait-before-run 1500ms \
    --settle-for 2s --print-gcounter counter:visits
# Terminal B
nx run counter.wasm --listen 127.0.0.1:9001 --peer 127.0.0.1:9000 \
    --datastore-path ./data-b --wait-before-run 1500ms \
    --settle-for 2s --print-gcounter counter:visits
# Both nodes print: counter:visits = 2
```

> Note: the internal wiring of Phase 6.5 is covered by E2E tests on `SyncManager`, including handshake, PushOps, convergence and sled materialization.
> The CLI criterion above is now covered by the Phase 7 lifecycle/smoke tests: startup hydration, settle mode, signal-aware shutdown, final flush and multi-process convergence.

---

### Phase 7: Graceful Lifecycle 🔄
**Goal**: Clean shutdown and recovery from crash

- [x] Robust long-running mode for the runtime with sync enabled.
- [x] Hydration on startup: rebuild the GCounter registry from the values
      materialized in sled.
- [x] Settle mode for `nx run` with sync enabled: give time to handshake,
      PushOps and remote apply before exit, or replace it with a long-running
      lifecycle.
- [x] Multi-process CLI smoke test: two `nx run distributed_counter.wasm`
      converge and print the same value within a few seconds.
- [x] Signal handling (SIGTERM, SIGINT, SIGHUP)
- [x] Graceful shutdown: complete in-flight ops, close connections
- [x] Store flush before exit
- [x] Configurable timeout for shutdown (default 30s)
- [x] Test: kill -TERM → no data corruption
- [x] Test: crash → restart → consistent state

**Remaining hardening:**
- [x] Read loops listen to the runtime shutdown signal instead of relying only
      on socket close/timeout.
- [x] Node shutdown waits a bounded time for network tasks to exit
      cooperatively before aborting them.
- [x] Test: active peer connections shut down without waiting for the socket
      read timeout.

**Criteria**:
```bash
kill -TERM $PID  # Completes operations, exits with code 0
```

---

### Phase 8: Backpressure and Limits ⚡
**Goal**: Stability under load

- [x] Peer connection limit (default: 64)
- [x] Queued ops limit (default: 10000)
- [x] Message size limit (default: 16 MiB)
- [x] Graceful rejection when overloaded
- [x] Socket read/write timeouts (default: 30s)
- [x] Test: 1000 simultaneous connections → no crash

**Configuration**:
```toml
[limits]
max_peers = 64
queued_ops_limit = 10000
max_message_size = "16MiB"
socket_timeout_secs = 30
```

---

### Phase 9: Observability 📊
**Goal**: Visibility into what the runtime is doing

**Structured logging**:
- [x] JSON format for logs
- [x] Configurable levels (trace/debug/info/warn/error)
- [x] Correlation ID to trace operations

**Metrics**:
- [x] `numax_ops_total` - Operations processed
- [x] `numax_peers_connected` - Active peers
- [x] `numax_sync_latency_ms` - Sync latency
- [x] `numax_store_keys` - Keys in the store
- [x] `numax_store_bytes` - Bytes used
- [x] `numax_sync_errors_total` - Sync errors
- [x] `numax_observability_requests_total` - Observability requests
- [x] `numax_observability_errors_total` - Observability request errors
- [x] `numax_peer_connects_total` - Peer connections observed
- [x] `numax_peer_disconnects_total` - Peer disconnections observed
- [x] `numax_broadcast_batches_total` - Broadcast batches sent
- [x] `numax_broadcast_ops_total` - Broadcast ops sent
- [x] `/metrics` endpoint (Prometheus format)

**Health check**:
- [x] `/health` endpoint (liveness)
- [x] `/ready` endpoint (readiness)
- [x] Test: `/ready` returns 503 before runtime readiness
- [x] Test: unknown observability paths return 404
- [x] Test: observability request timeout is bounded

**Configuration**:
```toml
[observability]
listen = "127.0.0.1:9100"
log_level = "info"
log_format = "text"
request_timeout_secs = 5
```

**Implementation**: `tracing`, `tracing-subscriber`, minimal Prometheus-compatible
HTTP endpoint over Tokio.

---

### Phase 10: Network Resilience 🌐
**Goal**: Robust operation with an unstable network

- [x] Automatic reconnect with exponential backoff
- [x] Peer health tracking (mark dead after N timeouts)
- [ ] Peer rotation (replace dead peers)
- [ ] Periodic anti-entropy (pull every N seconds)
- [ ] Op deduplication (bloom filter or set of OpIds)
- [ ] Durable CRDT state or op log, so restart/reconnect can recover full
      CRDT state rather than only materialized totals.
- [ ] Startup hydration from durable CRDT state/op log.
- [ ] Persist dedup metadata, or otherwise prevent duplicate remote ops after
      restart.
- [ ] Test: intermittent network (10% packet loss)
- [ ] Test: node dies and comes back → converges
- [ ] Test: duplicate op after restart does not double count

---

### Phase 11: Dual-Mode Serialization 📦
**Goal**: JSON for debugging, bincode for production

**Motivation**:
- JSON: readable, debuggable, inspectable with tcpdump/wireshark
- bincode: compact (~50% size), fast (~10x faster parse)

**Tasks**:
- [ ] Add `bincode` to dependencies
- [ ] `SerializationFormat` enum with a 1-byte header on the wire
- [ ] CLI flag `--debug-protocol`
- [ ] Format negotiation in Hello/HelloAck
- [ ] Test: roundtrip for both formats
- [ ] Benchmark: JSON vs bincode (size, speed)

**Libraries**: `bincode`, `serde` (already present)

---

### Phase 12: Extended Host API 🔌
**Goal**: Complete API for WASM modules

**Database**:
- [ ] `db_scan`, `db_exists`, `db_keys`

**Time**:
- [ ] `time_now`, `time_monotonic`

**Crypto**:
- [ ] `random_bytes`, `hash_sha256`, `hash_blake3`

**System**:
- [ ] `env_get`, `module_id`, `abort`

**Network**:
- [ ] `net_node_id`, `net_peers`

**Libraries**: `sha2`, `blake3`, `getrandom`

---

### Phase 13: Load Testing 🔥
**Goal**: Verify behavior under stress

**Scenarios**:
- [ ] Single node: 10k ops/sec for 1 hour
- [ ] 3 nodes: 1k ops/sec each, continuous sync
- [ ] 10 nodes: full mesh, 100 ops/sec each
- [ ] Chaos: kill a random node every 60s

**Metrics**: Throughput, p50/p95/p99 latency, RAM, CPU, convergence time.

**Tools**: custom script or `criterion`.

---

### Phase 14: Complete CRDTs 🧮
**Goal**: Data structures for real use cases

| CRDT | Description | Priority |
|------|-------------|----------|
| **PNCounter** | Counter with increment/decrement | High |
| **LWW-Register** | Single value, last-writer-wins | High |
| **ORSet** | Set with observed add/remove | High |
| **LWW-Map** | Key→value map with LWW | Medium |
| **RGA** | Replicated Growable Array (ordered lists) | Low |

For each one: implementation, property tests, OpKind, docs, example.

---

### Phase 15: Deployment & Docs 📦
**Goal**: Ready for external users

- [ ] Precompiled binaries (Linux x86_64, ARM64, macOS, Windows)
- [ ] `cargo install numax`
- [ ] Tutorial: "Distributed Hello World in 5 minutes"
- [ ] Tutorial: "Deploy 3+ nodes with mTLS"
- [ ] Guide: production configuration
- [ ] Guide: troubleshooting
- [ ] CHANGELOG.md
- [ ] CONTRIBUTING.md

---

## Phases Summary

| Phase | Name | Status | Priority |
|-------|------|--------|----------|
| 0-5 | Foundation | ✅ | - |
| 6 | Transport Security | ✅ | **P0** |
| 6.5 | End-to-End Sync Wiring | ✅* | **P0** |
| 7 | Graceful Lifecycle | ✅ | **P0** |
| 8 | Backpressure | ✅ | **P0** |
| 9 | Observability | ✅ | **P1** |
| 10 | Network Resilience | ⏳ | **P1** |
| 11 | Dual Serialization | ⏳ | **P1** |
| 12 | Extended Host API | ⏳ | **P1** |
| 13 | Load Testing | ⏳ | **P1** |
| 14 | Complete CRDTs | ⏳ | **P2** |
| 15 | Deployment & Docs | ⏳ | **P2** |

**Legend**:
- **P0**: Blocking for production
- **P1**: Required for safe production
- **P2**: Required for adoption

`✅*`: closed for internal wiring and `SyncManager` E2E tests; the literal CLI
criterion remains tracked in Phase 7 as lifecycle/settle/hydration.

---

## Final v0.1.0 Release Criteria

- [x] Phases 0-5 complete
- [x] Phase 6 (TLS) complete
- [x] Phase 6.5 (End-to-End Sync) complete
- [x] Phase 7 (Graceful shutdown) complete
- [x] Phase 8 (Backpressure) complete
- [x] Phase 9 (Observability) at least logging + health
- [ ] Phase 10 (Resilience) at least reconnect + dedup + durable CRDT recovery
- [ ] Phase 11 (Serialization) JSON + bincode working
- [ ] Phase 12 (Host API) at least db_scan, time_now, random_bytes
- [ ] Phase 13 (Load testing) at least the 3-nodes-1h scenario
- [ ] All tests pass
- [ ] No clippy warnings
- [ ] Base documentation

---

## v0.1.0-alpha.2 Release Criteria

- [x] Phases 0-5 complete
- [x] Phase 6 (TLS) complete
- [x] Phase 6.5 internal wiring covered by `SyncManager` E2E tests
- [x] Phase 7 graceful lifecycle complete
- [x] Base WASM examples present
- [x] `cargo test` passes outside the sandbox
- [x] `cargo clippy --all-targets --all-features -- -D warnings` passes
- [x] Known limitations documented in the roadmap

---

## v0.1.0-alpha.3 Release Criteria

- [x] Phase 7 graceful lifecycle hardening complete
- [x] Phase 8 backpressure complete
- [x] Phase 9 minimal observability complete
- [x] `cargo test` passes
- [x] `cargo clippy --workspace --all-targets -- -D warnings` passes
- [x] Known limitations documented in the roadmap

---

## 0.2.0:
> coming soon ...
