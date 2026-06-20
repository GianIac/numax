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
```

### Node lifecycle

```
Node::new(config)
  └── take_event_receiver()     take the event channel before starting
  └── start_listener()          bind TCP, spawn listener task, returns bound SocketAddr
  └── connect_to_peer(addr)     dial, TLS, handshake, register, spawn read loop
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
  1. acquire semaphore slot (PeerLimitReached if full)
  2. TCP connect with socket_timeout
  3. TLS handshake (if configured)
  4. capture peer_cert DER bytes
  5. send Hello { node_id, protocol_version, supported_formats, preferred_format }
  6. receive HelloAck { node_id, protocol_version, selected_format }
  7. validate protocol version == PROTOCOL_VERSION (3)
  8. if TLS and not insecure: derive NodeId from peer cert, verify == claimed node_id
  9. if allowlist configured: verify peer_node_id in allowed_peers
  10. insert PeerConnection into peers map
  11. emit PeerConnected event
  12. spawn read_loop task
```

### Inbound (listener)

```
handle_incoming(stream, addr, context)
  1. TLS accept (if configured), capture peer_cert
  2. receive Hello
  3. validate protocol version
  4. negotiate_serialization_format
  5. TLS identity binding (same as outbound)
  6. send HelloAck { node_id, protocol_version, selected_format }
  7. insert PeerConnection into peers map
  8. emit PeerConnected event
  9. run read_loop inline (not spawned - task already spawned by listener)
```

---

## Wire format

Every message is framed as:

```
[4 bytes BE length][1 byte format][payload bytes]
```

- Length is the total of `format byte + payload`, encoded as big-endian `u32`.
- Format byte: `0x01` = JSON, `0x02` = bincode.
- Payload is the serialized `Message` struct.

`PROTOCOL_VERSION = 3`. Version mismatch during handshake causes immediate disconnect.

### MessageKind variants

| Variant | Direction | Purpose |
|---|---|---|
| `Hello` | dialer -> listener | Open handshake: node identity, protocol version, supported formats |
| `HelloAck` | listener -> dialer | Accept handshake: protocol version and selected format |
| `PushOps` | both | Carry a batch of CRDT ops |
| `PushOpsAck` | both | Acknowledge reception count |
| `PullSince` | both | Request ops since a known op id (anti-entropy) |
| `Ping` / `Pong` | both | Keepalive |

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
    BincodeSerialization(Box<bincode::ErrorKind>),
    ConnectionFailed(String),
    PeerDisconnected(String),
    InvalidMessage(String),
    MessageTooLarge { len: usize, limit: usize },
    Timeout,
    ChannelClosed,
    TlsError(String),
    PeerNotAllowed(String),
    PeerLimitReached(usize),
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
- [Configuration](/numax/reference/configuration/) - TLS fields and limits that become `NodeConfig`
- [Crates overview](/numax/reference/crates/) - where `nx-net` fits in the dependency graph
