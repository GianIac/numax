---
title: Wire Versioning
description: Rules for evolving the Numax peer protocol safely.
---

## Purpose

`PROTOCOL_VERSION` identifies the wire contract used between Numax peers and is
independent from the Numax release version.

The current value is defined in `crates/nx-net/src/message.rs`.

The v0.1.6 development checkout uses `8`. Version `5`, shipped by v0.1.5,
introduced the one-shot bootstrap handshake described below. The development
protocol `6` introduced sequenced replication and bounded history exchange;
version `7` added required attempt identifiers to range requests, replay pages,
and unavailable-range responses. Version `8` adds bounded, versioned full CRDT
baseline recovery. Exact version matching rejects earlier development protocols
and v0.1.5 before peer registration or CRDT exchange.

Protocol 8 retains the private `History` variant after the existing private
bootstrap variants. Its messages carry sequenced operations, paginated
per-origin summaries, replay interval requests and pages, unavailable-range
responses, and baseline requests/results. The UUID `Op`, existing public `MessageKind` discriminants, and frozen
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

<span id="development-protocol-7-validation-and-deployment"></span>

## Compatibility verification and deployment

Compatibility verification applies to JSON and Bincode (`wincode`):

| Boundary | Required assertions |
| --- | --- |
| Incoming `Hello` / outgoing `HelloAck` | Exact version match; mismatch closes the socket before peer registration or CRDT events |
| Codec layout | Round trips for each variant and unchanged golden references for historical layouts |
| Baseline recovery | Correlated attempts, complete-cut validation, bounded payloads and explicit contract refusals |
| Superseded baseline failures | A late refusal cannot describe receipt coverage changed by another source |
| Storage failure | Original error cause remains observable; operational faults are not classified as invalid remote state |
| Previous binary | Protocol-5/protocol-8 rejection, separate local data, explicit migration and protected downgrade refusal |

Frozen protocol-7 history frames and older public codec fixtures remain
compatibility references. They describe their encoded layouts and do not grant
mixed-version admission.

The real-process entry point is
`different_protocol_versions_reject_connection_without_exchanging_ops` in
`crates/nx-cli/tests/multiprocess_smoke.rs`. CI builds the previous binary from
`refs/tags/v0.1.5` and checks its peeled commit against
`471a2281d0ffc8b7a3c20550320854812b8af726`. A mutable branch name does not identify
that release artifact. Reproducible commands and prerequisites are listed under
[Schema Versioning](/numax/design/schema-versioning/#compatibility-verification).

Exact matching interrupts replication between old and upgraded nodes during
deployment. Persisted-data migration does not translate wire traffic or provide
mixed-version rolling replication. The
[schema migration contract](/numax/design/schema-versioning/#upgrade-historical-state-and-downgrade-contract)
defines offline upgrade, downgrade protection and the historical baseline boundary.
Future baseline-format or membership additions follow the same versioning rules;
protocol 8 does not reserve compatibility for arbitrary snapshots.

## Protocol 8 bounded baseline recovery

Protocol 8 appends these variants to the private history exchange, preserving
all protocol-7 and older golden codec references:

| Message | Fields |
| --- | --- |
| `BaselineRequest` | attempt `request_id`, requested `origin`, baseline `version` |
| `Baseline` | matching `request_id` and `origin`, `version`, `payload`, 32-byte BLAKE3 `digest` |
| `BaselineUnavailable` | matching `request_id` and `origin`, bounded `reason` enum |

Baseline format 1 is independent of wire, package and store-schema versions.
The payload is JSON containing complete origin cuts and full internal state
records for GCounter, PNCounter, LwwRegister, LwwMap, ORSet and Rga. The outer
frame uses the negotiated JSON or Bincode (`wincode`) encoding. The digest
covers the exact payload bytes; it does not replace transport identity policy.
The existing admitted connection and configured TLS/allowlist policy apply.

The source must have complete known coverage, including its legacy baselines,
and flush the captured cut before responding. The receiver validates version,
digest, structure, budgets and a cut dominating its applied history and internal
state before atomic installation. The 1 MiB encoded payload, record/key limits,
RGA validation and refusal cases are specified in the
[D3 recovery contract](/numax/design/peer-discovery/#selected-bounded-recovery-contract).
This is a limited recovery subset, not a general snapshot or backup protocol.

Baseline attempts share the existing bounded per-peer/origin request map and
deadlines. Responses must match the live attempt. A negative result is recorded
only if the receiver's receipt coverage still matches the local fingerprint
captured at request time. Repair by another source, new known history or newly
installed legacy coverage invalidates that evidence. The fingerprint is local
request metadata and does not add wire fields or persisted schema records.

`UnsupportedVersion`, `SourceIncomplete`, `BudgetExceeded`, `CoverageConflict`
and `InvalidState` are contract rejections. Storage and metadata faults preserve
their original error chain and reach local sync-error logs/metrics; they are not
sent or recorded as one of those data rejections. If export fails operationally,
the source sends no misleading refusal and the request may expire and retry.
A failed import flush retains the uncertain-outcome fence until store reopen.

Exact matching rejects protocol 7 as well as v0.1.5 (protocol 5). Deployment
therefore requires the same explicit communication interruption and offline
schema migration as documented above; no mixed-version rolling replication is
promised. Reproduce the previous-binary check with the command in
[Schema Versioning](/numax/design/schema-versioning/#compatibility-verification).
