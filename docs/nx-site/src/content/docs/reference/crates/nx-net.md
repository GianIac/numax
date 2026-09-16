---
title: nx-net
description: Peer networking, wire messages and TLS.
---

`nx-net` owns everything below the sync layer: TCP connections, TLS/mTLS handshakes,
wire message framing, serialization format negotiation, peer slot management,
and the cooperative shutdown of all network tasks. It surfaces events upward to `nx-core`
via an async channel.

It depends on `nx-sync` for `Op` and `NodeId` types. It does not depend on `nx-core` or `nx-store`.

---

## Responsibilities

| Responsibility | Where |
|---|---|
| TCP listener and inbound connection handling | `node.rs` - `Node::start_listener`, `handle_incoming` |
| Outbound connections and handshake | `node.rs` - `Node::connect_to_peer` |
| Wire message framing (length-prefixed) | `node.rs` - `read_message`, `write_message` |
| Serialization format negotiation | `node.rs` - `negotiate_serialization_format` |
| TLS/mTLS acceptor and connector | `tls.rs` - `TlsConfig::accept_stream`, `TlsConfig::connect_stream` |
| NodeId binding to TLS certificate | `tls.rs` - `derive_protocol_node_id_from_cert` |
| Peer slot enforcement (semaphore) | `node.rs` - `connection_slots`, `ensure_peer_slot_available` |
| Broadcast and targeted op send | `node.rs` - `Node::broadcast_ops`, `Node::send_ops_to_addr` |
| Anti-entropy pull requests | `node.rs` - `Node::send_pull_since_to_addr` |
| Authenticated one-shot bootstrap exchange | `bootstrap.rs`, `node.rs` - `BootstrapClient`, inbound bootstrap handling |
| Bounded bootstrap advertisement cache | `bootstrap.rs` - `BootstrapServerConfig`, `BootstrapServer` |
| Cooperative shutdown via watch channel | `node.rs` - `Node::shutdown`, `shutdown_tx` |
| Wire message types and encode/decode | `message.rs` - `Message`, `MessageKind` |
| Peer state tracking | `node.rs` - `PeerConnection`, `peer.rs` - `PeerInfo`, `PeerState` |
| Error types | `error.rs` - `NetError` |

---

## Node

`Node` is the main struct. The sync manager in `nx-core` creates one, starts it, and consumes events from it.

```rust
pub struct Node {
    config:           NodeConfig,
    peers:            Arc<RwLock<HashMap<String, PeerConnection>>>,
    event_tx:         mpsc::Sender<NodeEvent>,
    event_rx:         Option<mpsc::Receiver<NodeEvent>>,
    shutdown_tx:      watch::Sender<bool>,
    connection_slots: Arc<Semaphore>,
    tasks:            Arc<Mutex<Vec<JoinHandle<()>>>>,
}
```

### NodeConfig

```rust
NodeConfig::new(node_id, "0.0.0.0:9000")
    .with_peers(vec!["127.0.0.1:9001".into()])
    .with_tls(tls_config)
    .with_max_peers(64)
    .with_max_message_size(16 * 1024 * 1024)
    .with_socket_timeout(Duration::from_secs(30))
    .with_serialization_format(SerializationFormat::Bincode)
    .with_event_channel_capacity(1024)
    .with_bootstrap_server(BootstrapServerConfig::new("cluster-a")?)
```

`NodeConfig::validate()` checks limits before channel/semaphore allocation or
network startup: `max_peers` cannot exceed Tokio's semaphore capacity,
`event_channel_capacity` must be positive and within that capacity, and
`socket_timeout` must be positive and form a representable deadline.
`max_peers = 0` is valid and disables connection admission.

Prefer `Node::try_new(config)`, which returns `NetError::InvalidConfig` for these
invalid limits without binding sockets. The legacy infallible `Node::new(config)`
remains available: an invalid configuration produces an inert node whose network
entry points reject it, not a working node with silently clamped limits.
Validation does not establish that a listen address can be bound or a peer reached.

### Node lifecycle

```
Node::try_new(config)?          validate before constructing; no socket binding yet
  └── take_event_receiver()     take the event channel before starting
  └── start_listener()          bind TCP, spawn listener task, returns bound SocketAddr
  └── connect_to_peer(addr)     dial, TLS, handshake, register, spawn read loop
  └── connection_info(addr)     transport, direction and verified/claimed identity
  └── announce_bootstrap_endpoint(addr)  publish the local bootstrap suggestion
  └── withdraw_bootstrap_endpoint()      remove that suggestion
      ...running...
  └── broadcast_ops(ops)        push ops to all connected peers
  └── send_ops_to_addr(addr, ops)
  └── send_pull_since_to_addr(addr, since_op_id)
  └── shutdown()                sends true on shutdown_tx, waits for tasks (3s grace), drops peers
```

### NodeEvent

Events emitted to the sync manager via `mpsc::Sender<NodeEvent>`:

```rust
pub enum NodeEvent {
    OpsReceived     { from: NodeId, ops: Vec<Op> },
    PullRequested   { from: NodeId, addr: String, since_op_id: Option<String> },
    PeerConnected   { node_id: NodeId, addr: String, peers_connected: usize },
    PeerDisconnected{ node_id: NodeId, addr: String, peers_connected: usize },
}
```

`take_event_receiver()` must be called once before `start_listener`. The receiver is moved out
of the `Node` so the sync manager owns it.

---

## Connection flow

### Outbound (dialer)

```
connect_to_peer(addr)
  1. reject a duplicate attempt for the same endpoint and acquire the bounded outbound-attempt slot
  2. acquire connection semaphore slot (PeerLimitReached if full)
  3. TCP connect with socket_timeout and retain the actual transport address
  4. TLS handshake (if configured)
  5. capture peer_cert DER bytes
  6. send Hello { node_id, protocol_version, supported_formats, preferred_format }
  7. receive HelloAck { node_id, protocol_version, selected_format }
  8. validate protocol version == PROTOCOL_VERSION (5) and reject the local NodeId
  9. if TLS and not insecure: derive NodeId from peer cert, verify == claimed node_id
  10. if allowlist configured: verify peer_node_id in allowed_peers
  11. insert PeerConnection and its `PeerConnectionInfo` into the peers map
  12. emit PeerConnected event and spawn the read loop
```

### Inbound (listener)

```
handle_incoming(stream, addr, context)
  1. TLS accept (if configured), capture peer_cert
  2. receive Hello or BootstrapHello
  3. validate protocol version and negotiate_serialization_format
  4. reject the local NodeId, then perform TLS identity binding and allowlist checks
  5a. normal: send HelloAck, insert PeerConnection, emit PeerConnected, run read_loop
  5b. bootstrap: validate service/cluster/request, send BootstrapAck, close without peer admission
```

`PeerConnectionInfo` keeps the TCP transport address separate from the outbound
endpoint that was dialed. It also records inbound/outbound direction and whether
the handshake NodeId was certificate-bound or unverified. These are runtime
facts only and do not change the wire format.

---

## Wire format

Every message is framed as:

```
[4 bytes BE length][1 byte format][payload bytes]
```

- Length is the total of `format byte + payload`, encoded as big-endian `u32`.
- Format byte: `0x01` = JSON, `0x02` = bincode.
- Payload is the serialized `Message` struct.

`PROTOCOL_VERSION = 5`. Version mismatch during a recognized handshake causes a structured
`WireError::ProtocolMismatch` and immediate disconnect.

### MessageKind variants

| Variant | Direction | Purpose |
|---|---|---|
| `Hello` | dialer -> listener | Open handshake: node identity, protocol version, supported formats |
| `HelloAck` | listener -> dialer | Accept handshake: protocol version and selected format |
| `PushOps` | both | Carry a batch of CRDT ops |
| `PushOpsAck` | both | Acknowledge reception count |
| `PullSince` | both | Request ops since a known op id (anti-entropy) |
| `Ping` / `Pong` | both | Keepalive |
| `Error` | both | Structured wire error: `ProtocolMismatch`, `OpRejected`, `RateLimited`, `NotAuthorized`, `Internal` |
| `BootstrapHello` | client -> seed | One-shot identity, format, cluster, optional endpoint advertisement and result limit |
| `BootstrapAck` | seed -> client | Seed identity, format, cluster, bounded candidates and lease |

### WireError semantics

| Error | Retry policy | Meaning |
|---|---|---|
| `ProtocolMismatch` | Fatal | Different wire contracts. Upgrade/downgrade one side before reconnecting. |
| `NotAuthorized` | Fatal for that peer/config | Credentials, certificate identity, or allowlist must change before retrying. |
| `RateLimited` | Retryable | Back off. Use `retry_after_ms` when present, otherwise use normal reconnect backoff. |
| `OpRejected` | Fatal for those ops | Do not resend the same rejected ops unchanged. Current generic error handling closes the peer connection. |
| `BootstrapRejected` | Fatal for that request | Bootstrap is disabled or its cluster, advertisement or request bounds are invalid. |
| `Internal` | Retryable with backoff | Treat as transient unless it repeats; record metrics/logs. |

The configured-peer reconnect loop uses this policy: fatal wire errors stop
automatic reconnect for that peer, `RateLimited.retry_after_ms` is honored up to
the configured reconnect max delay, and retryable errors keep the normal
exponential backoff.

### Serialization format negotiation

When a bincode node connects to a JSON-only debug node:

```
dialer sends: supported_formats = [Bincode, Json], preferred = Bincode
listener picks: first format in dialer's list that listener supports
result: Json (because listener only supports Json)
HelloAck selected_format = Json
```

A `--debug-protocol` node (JSON only) always negotiates JSON with any peer.
A standard node advertises both and prefers bincode.

### Bootstrap transport

`BootstrapClient::query(seed, request)` opens a bounded, one-shot connection,
sends `BootstrapHello`, authenticates the `BootstrapAck` seed identity and
returns `BootstrapResponse`. `BootstrapClientConfig` reuses `NodeId`, optional
`TlsConfig`, message-size, socket-timeout and serialization controls. Its
defaults permit one concurrent query, at most 128 returned candidates and a
maximum accepted candidate TTL of 300s.

The server is enabled through `NodeConfig::with_bootstrap_server`. By default it
retains at most 1024 authenticated requester advertisements for 60s and returns
at most 128 candidates. Its own advertised endpoint is returned first, followed
by cached requester endpoints in stable insertion order; responses are
deduplicated and exclude the current requester. A request without an advertised
endpoint withdraws that requester's cache entry.

Cluster IDs, endpoint strings, response count and leases are validated on both
sides. A bootstrap socket holds an inbound connection slot while the request is
processed but is never inserted into the active peer map, never enters a read
loop and never emits `PeerConnected`. Suggestions in `BootstrapResponse` are
not authenticated identities; the normal dialer must authenticate each one in
a separate `Hello`/`HelloAck` exchange.

---

## TLS and mTLS

TLS is optional. When `TlsConfig` is provided:

- Outbound: `TlsConfig::connect_stream(tcp, server_name)` via `tokio-rustls`.
- Inbound: `TlsConfig::accept_stream(tcp)`.
- Both sides extract the peer DER certificate from the completed TLS session.

### TlsConfig

```rust
pub struct TlsConfig {
    pub cert_path:     Option<String>,  // this node's PEM cert
    pub key_path:      Option<String>,  // this node's PEM key
    pub ca_path:       Option<String>,  // CA cert for peer verification (enables mTLS)
    pub allowed_peers: Option<HashSet<String>>, // optional allowlist of NodeId strings
    pub insecure:      bool,            // skip cert verification (dev only)
}
```

### NodeId binding

After TLS handshake, the node verifies the claimed NodeId in `Hello`/`HelloAck` against
the identity derived from the peer's X.509 certificate:

```
derive_protocol_node_id_from_cert(peer_cert_der)
  -> SHA-256 of SubjectPublicKeyInfo bytes
  -> first 16 hash bytes
  -> 32 lowercase hex chars
  -> NodeId(hex_prefix)
```

If the claimed NodeId does not match the cert-derived one, the connection is rejected with
`NetError::TlsError("node_id mismatch ...")`.

### Allowlist enforcement

When `TlsConfig.allowed_peers` is set, the cert-derived NodeId is checked against the set.
A peer not in the allowlist is rejected after the TLS handshake, before ops are exchanged.

### Test utilities

`TestPki` in `tls.rs` generates an in-memory CA + two node certs for use in tests:

```rust
let pki = TestPki::generate().unwrap();
let node1_cfg = pki.node1_config(); // TlsConfig for node 1
let node2_cfg = pki.node2_config(); // TlsConfig for node 2
```

---

## Peer slot management

Peer capacity is enforced with a `tokio::sync::Semaphore` initialized to `max_peers`.

- Inbound: `try_acquire_owned()` at accept time - the permit is held in `PeerConnection._slot`.
  If the semaphore is exhausted, the connection is dropped before the TLS/handshake cost.
- Outbound: `try_acquire_owned()` before TCP connect - fails fast with `PeerLimitReached`.

The permit is dropped when the `PeerConnection` is removed from the peers map (on disconnect or shutdown).

---

## Cooperative shutdown

Shutdown uses a `tokio::sync::watch` channel. `shutdown_tx` is a `watch::Sender<bool>`.
All background tasks subscribe with `shutdown_tx.subscribe()` and select on `shutdown_rx.changed()`.

```
Node::shutdown()
  1. shutdown_tx.send(true)
  2. collect all JoinHandles from tasks Vec
  3. for each task: timeout(3s, task).await
     - if task does not finish in 3s: task.abort()
  4. peers.clear() -> drops all PeerConnection -> drops all semaphore permits
  5. clear the bootstrap advertisement and leased requester cache
```

Read loops check the shutdown signal on every iteration via `tokio::select!`.
Listener loop checks it between accept calls.
This avoids waiting for socket timeouts during clean shutdown.

---

## Error types

```rust
pub enum NetError {
    Io(std::io::Error),
    Serialization(serde_json::Error),
    BinarySerialization(wincode::WriteError),
    BinaryDeserialization(wincode::ReadError),
    ConnectionFailed(String),
    PeerDisconnected(String),
    InvalidConfig(String),
    InvalidMessage(String),
    Wire(WireError),
    MessageTooLarge { len: usize, limit: usize },
    Timeout,
    ChannelClosed,
    TlsError(String),
    PeerNotAllowed(String),
    PeerLimitReached(usize),
    ConnectionAttemptLimitReached(usize),
    ConnectionInProgress(String),
    SelfConnection(String),
    NodeIdMismatch { expected: String, got: String },
}
```

---

## Defaults

| Constant | Value | Description |
|---|---|---|
| `DEFAULT_MAX_PEERS` | 64 | Maximum simultaneous peers |
| `DEFAULT_MAX_MESSAGE_SIZE` | 16 MiB | Maximum wire message size |
| `DEFAULT_SOCKET_TIMEOUT` | 30s | Read/write timeout per operation |
| `DEFAULT_EVENT_CHANNEL_CAPACITY` | 1024 | Event channel buffer size |
| `DEFAULT_BOOTSTRAP_CACHE_CAPACITY` | 1024 | Seed-side advertised endpoint cache |
| `DEFAULT_BOOTSTRAP_RESPONSE_CAPACITY` | 128 | Results returned by one bootstrap exchange |
| `MAX_BOOTSTRAP_RESPONSE_CAPACITY` | 4096 | Hard limit for one bootstrap response, not the combined discovery snapshot |
| `DEFAULT_BOOTSTRAP_CANDIDATE_TTL` | 60s | Seed-side advertisement lease |
| `DEFAULT_MAX_CONCURRENT_BOOTSTRAP_QUERIES` | 1 | Simultaneous queries per bootstrap client |
| `TASK_SHUTDOWN_GRACE` | 3s | Cooperative shutdown grace per task |

---

## Test coverage

Tests live in `node.rs` and `message.rs` (`#[cfg(test)]`), plus integration tests in `tests/`.

| Test | What it covers |
|---|---|
| `test_node_config` | default values and builder |
| `node_config_allows_custom_peer_limit` | `with_max_peers` |
| `negotiation_prefers_local_format_when_peer_supports_it` | bincode-bincode -> bincode |
| `negotiation_falls_back_to_json_for_debug_peer` | bincode node + json-only peer -> json |
| `negotiation_rejects_empty_peer_formats` | no common format -> None |
| `peer_slot_limit_rejects_new_peer_when_full` | semaphore exhausted -> PeerLimitReached |
| `peer_slot_limit_allows_replacing_same_addr` | re-connect to same addr is allowed |
| `mark_peer_failed_returns_updated_connected_count` | peer state transition + count |
| `track_task_prunes_finished_handles_before_push` | task list stays clean |
| `is_connected_addr_tracks_connected_state` | only Connected peers return true |
| `read_message_rejects_payload_over_configured_limit` | MessageTooLarge |
| `read_message_times_out_waiting_for_length` | Timeout on stalled read |
| `connect_to_peer_times_out_during_handshake` | Timeout during plain handshake |
| `connect_to_peer_times_out_during_tls_handshake` | Timeout during TLS handshake |
| `connect_to_peer_rejects_protocol_version_mismatch` | old version in HelloAck |
| `incoming_rejects_protocol_version_mismatch` | old version in Hello |
| `protocol_v5_binary_encoding_matches_bincode_golden_hashes` | stable binary encoding for normal and bootstrap messages |
| `one_shot_query_returns_candidates_without_registering_a_peer` | bootstrap response without active peer admission or events |
| `cluster_mismatch_is_rejected_without_populating_the_cache` | cluster isolation before advertisement caching |
| `incoming_idle_handshake_consumes_peer_slot` | slot held before handshake completes |
| `incoming_idle_tls_handshake_releases_peer_slot_after_timeout` | slot released after timeout |
| `active_peer_shutdown_does_not_wait_for_socket_timeout` | cooperative shutdown timing |
| `incoming_ping_gets_pong_response` | Ping/Pong keepalive |
| `bincode_node_negotiates_json_with_debug_peer` | cross-format negotiation E2E |
| `message_roundtrip_json` / `message_roundtrip_bincode` | encode/decode roundtrip |
| `rejects_unknown_serialization_format` | unknown format byte -> InvalidMessage |

```bash
cargo test -p nx-net
```

---

## Related

Use this page together with the sync model and runtime docs:

- [nx-sync crate](/numax/reference/crates/nx-sync/) - `Op` and `NodeId` types used by the wire protocol
- [nx-core crate](/numax/reference/crates/nx-core/) - the sync manager that drives `Node`
- [Configuration](/numax/reference/config/) - TLS fields and limits that become `NodeConfig`
- [Crates overview](/numax/reference/crates/) - where `nx-net` fits in the dependency graph
