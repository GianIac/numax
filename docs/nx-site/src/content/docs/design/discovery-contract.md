---
title: Peer Discovery Contract
description: Snapshot, event delivery, cancellation, and compatibility guarantees for peer discovery providers.
---

## Scope and ownership

The peer discovery abstraction belongs to `nx-core`. It supplies peer endpoint
candidates to runtime orchestration without moving connection management,
authentication, or wire-protocol concerns into discovery providers.

`PeerDiscovery` defines three operations:

- `discover()` returns the provider's current snapshot;
- `watch()` subscribes to changes after that snapshot;
- `announce()` asks a provider to publish the local endpoint when it supports
  announcements.

This contract covers the abstraction and `StaticDiscovery` only. Dynamic
providers and the coordination that applies candidates to the live peer set are
separate roadmap items.

## Snapshot and watch consistency

Creating a watch and reading the snapshot bundled with it are one atomic
observation. An update cannot occur between those actions without being
represented either in that snapshot or by a subsequent event. Consumers that
need updates therefore start with `DiscoveryWatch::snapshot()` and then process
the same watch's event stream. The separate `discover()` method is for
point-in-time reads and must not be combined with a later `watch()` call.

`StaticDiscovery` is immutable. Its snapshot preserves the configured peer list
exactly, including input order and duplicate entries. Its watch produces no
change events.

## Bounded event delivery

Watch delivery is bounded. A provider must not grow an unbounded queue when a
consumer is slow. If changes exceed the available capacity, overflow is exposed
to the consumer as an explicit provider error rather than silently dropping
events. Revisions are contiguous and strictly increasing after the watch
snapshot; a discontinuity is also an explicit error. After either condition,
incremental state is no longer authoritative and the consumer must create a
new watch and use its bundled snapshot before continuing.

Dropping a watch cancels that subscription. Provider closure terminates the
watch. `StaticDiscovery` owns no background task, so dropping it or its watch
requires no asynchronous shutdown or task join.

## Announcements and errors

Announcement support is a provider capability. `StaticDiscovery::announce()`
returns the explicit unsupported-operation error; it does not silently succeed
and does not alter the configured snapshot. Other provider failures are returned
through the typed discovery error boundary so callers can distinguish an
unsupported capability, closed delivery, and overflow requiring a resnapshot.

## Security and compatibility boundaries

A discovered endpoint is only a connection candidate. Discovery does not assert
node identity, authenticate a peer, authorize a connection, or establish
membership. Existing TLS and mTLS verification, peer allowlists, connection
limits, and handshake checks remain authoritative when the runtime attempts a
connection.

The abstraction and `StaticDiscovery` do not change peer messages, framing,
handshake semantics, persisted data, or the WebAssembly host and guest APIs.
They therefore require no wire-protocol version increment, storage migration,
or guest ABI change.

Bootstrap exchange, mDNS, DNS-SRV, file watching, endpoint expiry and removal,
and candidate coordination with reconnection and anti-entropy are outside this
contract's scope.
