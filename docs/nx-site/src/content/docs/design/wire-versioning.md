---
title: Wire Versioning
description: Rules for evolving the Numax peer protocol safely.
---

## Purpose

`PROTOCOL_VERSION` identifies the wire contract used between Numax peers and is
independent from the Numax release version.

The current value is defined in `crates/nx-net/src/message.rs`.

In `v0.1.5`, the current Numax version, the value is `5`. Version `5` adds the
one-shot bootstrap handshake described below; it is not wire-compatible with
the version `4` protocol shipped by `v0.1.4`.

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

Protocol `5` adds `BootstrapHello` and `BootstrapAck` after the existing
`MessageKind` variants and adds `WireError::BootstrapRejected`. A bootstrap
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

Both JSON and Bincode round trips, exact-version rejection and Bincode golden
hashes cover the version `5` message set. Multiprocess compatibility coverage
uses the previous `v0.1.4` binary to verify safe rejection at the normal
handshake boundary.

CI resolves the previous binary's source from the explicit
`refs/tags/v0.1.4` reference and verifies its peeled commit is
`419d840e2afe780e7ad1f4135e39e9b38a4f30b1` before building it. A branch with the
same short name is not an acceptable substitute. This test checks rejection,
not mixed-version replication or unrestricted recovery after history expiry.
