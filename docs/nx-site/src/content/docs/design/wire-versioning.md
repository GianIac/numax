---
title: Wire Versioning
description: Rules for evolving the Numax peer protocol safely.
---

## Purpose

`PROTOCOL_VERSION` identifies the wire contract used between Numax peers and is
independent from the Numax release version.

The current value is defined in `crates/nx-net/src/message.rs`.

The v0.1.6 development checkout uses `7`. Version `5`, shipped by v0.1.5,
introduced the one-shot bootstrap handshake described below. The development
protocol `6` introduced sequenced replication and bounded history exchange;
version `7` adds required attempt identifiers to range requests, replay pages,
and unavailable-range responses. Exact version matching rejects both protocol
`6` and v0.1.5 before peer registration or CRDT exchange.

Protocol 7 uses a private `History` variant after the existing private
bootstrap variants. Its messages carry sequenced operations, paginated
per-origin summaries, replay interval requests and pages, and unavailable-range
responses. The UUID `Op`, existing public `MessageKind` discriminants, and frozen
v0.1.4/v0.1.5 codec fixtures are preserved. A history summary advertises known
existence; it is neither a contiguous receipt frontier nor a durability ACK.
Range responses must match the pending source connection, origin, request ID,
unexpired deadline and requested interval. Retried requests receive fresh IDs;
late responses cannot complete or mark unavailable a later attempt.

## Compatibility policy

Numax currently requires an exact version match:

| Local protocol | Peer protocol | Result |
|---|---|---|
| `N` | `N` | Accept |
| `N` | `N - 1` | Reject |
| `N` | `N + 1` | Reject |

Both `Hello` and `HelloAck` carry `protocol_version`. A mismatch must terminate
the handshake before registering the peer or exchanging CRDT operations.

## When to increment

Increment `PROTOCOL_VERSION` when a change can make two nodes decode or
interpret the same wire exchange differently.

This includes:

- adding, removing, renaming, or reordering fields in bincode-encoded messages;
- changing a field type, required value, or meaning;
- adding, removing, or reordering `MessageKind` variants;
- changing framing, format identifiers, or serialization configuration;
- changing handshake, acknowledgement, or error semantics;
- changing the wire representation or meaning of CRDT operations;
- introducing a mandatory authentication or negotiation step.

Do not increment it for:

- internal refactoring with identical serialized output and semantics;
- logging, metrics, documentation, or test-only changes;
- performance improvements that preserve the wire contract;
- bug fixes that restore already documented behavior without changing the
  accepted or emitted messages.

When uncertain, serialize representative messages before and after the change
and compare both their bytes and interpretation. If either differs
incompatibly, increment the version.

## Required change procedure

A breaking wire change must include all of the following in the same pull
request:

1. Increment `PROTOCOL_VERSION`.
2. Update `Hello` and `HelloAck` tests.
3. Update protocol mismatch and compatibility-matrix tests.
4. Run the multiprocess E2E test against the previous release binary.
5. Update this document and the gossip protocol documentation.
6. Record the new protocol version and compatibility policy in release notes.

The multiprocess test must prove that incompatible binaries:

- report a protocol-version mismatch;
- do not register each other as peers;
- do not exchange CRDT operations;
- exit without panic or data corruption.

## Serialization rules

Numax supports bincode and JSON, compatibility must be evaluated for both:

- bincode depends on enum variant order and field order;
- JSON depends on field and variant names;
- serde aliases may help decode an older JSON shape, but they do not make
  different protocol semantics compatible;
- serialization-format negotiation happens only after protocol compatibility
  is established.

Never reuse a protocol version for a different wire contract.

## Protocol 5 bootstrap exchange

Protocol `5` adds private wire variants `BootstrapHello` and `BootstrapAck` after
the legacy public `MessageKind` layout and adds a private
`BootstrapRejected` wire error after the legacy public `WireError` layout. A bootstrap
exchange is an alternative one-shot handshake on the normal peer listener; it
does not turn into a replication connection.

```text
client -> seed: BootstrapHello {
  node_id,
  protocol_version: 5,
  supported_formats,
  preferred_format,
  cluster_id,
  advertised_endpoint?,
  max_results
}

seed -> client: BootstrapAck {
  node_id,
  protocol_version: 5,
  selected_format,
  cluster_id,
  candidates,
  candidate_ttl_ms
}
```

The seed validates the exact protocol version, negotiates JSON or Bincode,
authenticates the requester's claimed `NodeId` through the same TLS certificate
binding and allowlist policy as a normal handshake, and requires an exact
cluster ID match. It validates any advertised endpoint before caching it. The
request's `max_results`, the server response limit and the server cache limit
bound the exchange independently.

The exported `nx_net::MAX_BOOTSTRAP_RESPONSE_CAPACITY` is `4096`. Client and
server response capacities must be in `1..=4096`; the effective CLI
`discovery.max_candidates` obeys this upper bound only in bootstrap mode.
This resource limit is independent of the wire version, package version, cache
capacity and message-byte limit.

The client validates the seed's protocol version, selected format, cluster ID,
authenticated identity, response length, candidate lease and every endpoint.
Duplicate endpoints, wildcard hosts, port zero and malformed responses reject
the complete response. Successful completion closes the one-shot connection;
it neither registers the seed as an active replication peer nor emits a peer
connection event.

Only the requester's identity and the responding seed's identity are covered by
that exchange. The returned endpoint strings are untrusted discovery
candidates. Dialing one later requires a new normal `Hello`/`HelloAck`, TLS
identity check, allowlist decision and connection-slot admission.

### Version 4 boundary

Normal `Hello` exchanges between versions `4` and `5` carry a readable version
field and are rejected with `ProtocolMismatch` before peer registration or CRDT
traffic. A version `4` decoder does not know the new bootstrap variants and may
close a `BootstrapHello` as an invalid message rather than returning a
structured mismatch; this is still a safe rejection and never admits a peer.
Static peer configuration remains source-compatible but does not make mixed
version `4`/`5` clusters wire-compatible.

JSON and Bincode round trips cover the complete private version `5` message set.
Bincode golden hashes and direct public/private byte comparisons protect every
legacy public variant. They preserve encoding references; they do not grant
mixed-version admission.

## Development protocol 7 validation and deployment

Unit handshake tests cover incoming `Hello` and outgoing `HelloAck` rejection
for versions 5, 6 and 8 in both JSON and Bincode. They assert the version error,
closed socket, zero admitted peers and absence of connection/CRDT events.
History codec tests cover every protocol-7 message variant in both formats,
with frozen SHA-256 references for complete frames as well as round trips.

The real-process compatibility test is
`different_protocol_versions_reject_connection_without_exchanging_ops` in
`crates/nx-cli/tests/multiprocess_smoke.rs`. It runs both JSON and Bincode,
checks the protocol-5/protocol-7 mismatch and separate local counter values,
then checks explicit store migration, preserved values plus a new write, and
old-runtime downgrade rejection followed by a successful reopen and new write
on the protected store. Unit admission assertions complement the
real-process data-isolation assertions; no single counter check alone proves
absence of peer registration.

CI builds the previous binary from `refs/tags/v0.1.5` and verifies its peeled
commit is `471a2281d0ffc8b7a3c20550320854812b8af726`. A similarly named branch
is not a substitute. The older v0.1.4 codec fixtures remain frozen references.

Exact matching interrupts replication between old and upgraded nodes during
deployment. Persisted-data migration does not translate wire traffic or provide
mixed-version rolling replication. See [Schema Versioning](/numax/design/schema-versioning/)
for offline upgrade, downgrade protection and the historical baseline boundary.
Future state-transfer or membership wire additions must follow the same version
procedure; protocol 7 does not implicitly reserve a compatible transfer format.
