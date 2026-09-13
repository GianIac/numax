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

`DiscoveryProvider` gives every source a stable source ID and an optional
candidate lease. `DiscoveryRuntimeConfig` defines the local cluster scope, the
optional advertised endpoint, and the global candidate bound. The coordinator
owns every watch and is the only component that turns provider contributions
into the effective candidate snapshot.

## Snapshot and watch consistency

Creating a watch and reading the snapshot bundled with it are one atomic
observation. An update cannot occur between those actions without being
represented either in that snapshot or by a subsequent event. Consumers that
need updates therefore start with `DiscoveryWatch::snapshot()` and then process
the same watch's event stream. The separate `discover()` method is for
point-in-time reads and must not be combined with a later `watch()` call.

`StaticDiscovery` is immutable. Its snapshot preserves the configured peer list
exactly, including input order and duplicate entries. Its watch produces no
change events. The coordinator canonicalizes endpoints and keeps the first
occurrence order, so duplicate configuration entries still result in only one
connection candidate.

## Candidate ownership, expiry, and removal

An effective candidate can have contributions from multiple discovery sources.
`Added` refreshes that source's optional lease. `Removed` deletes only that
source's contribution; the candidate disappears only after its last source is
removed or expires. A leased source survives a watch failure until its lease
expires, while an unleased source is removed when its watch becomes unavailable.
A successful resubscription atomically replaces that source from the new watch
snapshot.

The resulting bounded snapshot is shared by initial dialing, automatic
reconnection, and anti-entropy. All three preserve its order. An empty startup
snapshot is valid, and the loops remain alive for later additions. Removing a
candidate immediately stops new reconnect attempts and anti-entropy requests;
it does not terminate an already active, admitted connection. Once that
connection closes it is not re-established unless a source adds the endpoint
again.

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

Providers declare announcements unsupported, optional, or required. Required
announcements make startup fail if no dialable local endpoint can be derived.
Provider `shutdown()` owns withdrawal of announcements and termination of any
provider-internal work. The coordinator stops and joins every watch task and
calls every provider shutdown hook during normal shutdown and partial-startup
rollback. Provider operations have a finite timeout so a stuck implementation
cannot keep runtime shutdown alive indefinitely.

## Endpoints, identity, and connection admission

Four values remain deliberately separate:

- a discovery candidate is an untrusted endpoint suggestion;
- an advertised endpoint is the address the local node asks providers to
  publish;
- a transport address is the actual remote TCP endpoint of an active socket;
- a peer identity is the `NodeId` learned in the handshake together with its
  verification level (`CertificateBound` or `Unverified`).

For outbound connections Numax also retains the candidate that was dialed. An
inbound connection has no dialed candidate. Discovery never promotes an
endpoint into an authenticated identity or an active connection.

Candidate ports must be non-zero and unspecified IP addresses such as
`0.0.0.0` and `::` are rejected. When the listener uses port zero, an explicit
advertised endpoint with port zero inherits the actual bound port. A wildcard
bind cannot be announced without an explicit non-wildcard advertised host. Both
the concrete bind address and advertised endpoint are excluded from candidates
when available.

Self endpoints are filtered before dialing, and a connection claiming the
local `NodeId` is rejected after the handshake. Candidate duplicates are
collapsed, concurrent outbound attempts are globally limited to one, and a
second attempt to the same endpoint is rejected while the first is pending.
Active and in-progress connections share the existing `max_peers` semaphore.
Simultaneous connections arriving through different transport addresses remain
distinct and each consumes a slot; no nondeterministic identity-based winner is
selected without a protocol-level connection nonce.

The default candidate bound is 1024 and is configurable through
`DiscoveryRuntimeConfig`. Reconnect retains its existing per-endpoint backoff
and fatal wire-error policy. Anti-entropy retains its existing bounded op-log
pull and deduplication behavior.

## Cluster isolation

Each provider reports the logical cluster it serves. Startup rejects a provider
whose cluster differs from the runtime cluster, and duplicate source IDs are
invalid. Provider implementations must scope all snapshots, changes, and
announcements to that cluster. The cluster value is a discovery routing scope,
not proof of membership and not a replacement for TLS identity or authorization;
it is intentionally not added to the current wire handshake.

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

Bootstrap exchange, mDNS, DNS-SRV, and file watching remain provider-specific
roadmap work outside this contract.
