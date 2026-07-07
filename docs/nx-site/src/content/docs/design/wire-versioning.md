---
title: Wire Versioning
description: Rules for evolving the Numax peer protocol safely.
---

## Purpose

`PROTOCOL_VERSION` identifies the wire contract used between Numax peers and It is
independent from the Numax release version.

The current value is defined in `crates/nx-net/src/message.rs`.

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
