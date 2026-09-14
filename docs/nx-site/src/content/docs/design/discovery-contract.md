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

`StaticDiscovery` is immutable. Its provider snapshot preserves the configured
peer list exactly, including input order and duplicate entries, and its watch
produces no change events. The coordinator canonicalizes endpoints and keeps
the first occurrence order, so duplicate configuration entries still result in
only one effective connection candidate. Invalid legacy `--peer` values are
logged and skipped instead of making discovery startup fail.

Dynamic providers keep a complete ordered view and publish bounded,
revisioned `Replaced` events. A replacement changes the provider contribution
atomically, including its ordering: consumers never observe a synthetic empty
view between removals and additions. `Added` and `Removed` remain available for
incremental providers. Providers deduplicate their own snapshots where their
source naturally can repeat endpoints; the coordinator also deduplicates
across providers. Ordering is deterministic for a given set of provider
observations, but it is not a membership or authorization guarantee.

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

`shutdown()` is idempotent. After shutdown, a provider cannot be restarted.
Dropping a provider is also a cancellation boundary: implementations that own
background work signal or abort it rather than leaving detached discovery
activity alive.

## Provider contracts

All provider limits are checked before a view is exposed to the coordinator.
The runtime-wide candidate limit remains an additional bound after different
sources are combined.

### StaticDiscovery

`StaticDiscovery::new(peers)` is the compatibility adapter for configured
peers. It performs no I/O, never refreshes or expires entries, preserves the
input list byte-for-byte, and does not support announcements. An empty list is
valid.

### BootstrapGossipDiscovery

`BootstrapGossipDiscovery` contacts a bounded, ordered seed list through the
one-shot `BootstrapHello`/`BootstrapAck` exchange. Startup with no responses is
valid: the initial provider snapshot is empty and probing continues in the
background. Seed addresses are canonicalized and deduplicated while retaining
their first configured occurrence.

Each request optionally advertises the caller's endpoint and asks for at most
the configured number of results. A successful view contains the seed itself
followed by the seed's bounded, deduplicated suggestions. Views from multiple
seeds are flattened in configured seed order and deduplicated again. Returned
entries expire at the earlier of the seed-provided lease and the provider's
`stale_after` bound. Failed probes retain an unexpired last valid view; expired
views are removed at their deadline even while another seed query is still in
flight. Each successful seed response is published without waiting for the
remaining seeds in the refresh pass.

Probe failures use exponential retry bounded by `retry_initial` and
`retry_max`; a success restores `refresh_interval`. Fatal wire failures such as
protocol mismatch or bootstrap request rejection disable that seed for the
provider lifetime. Bootstrap announcement support is required. Shutdown stops
and joins the probe loop, performs bounded best-effort withdrawal from every
seed that accepted the announcement, and clears the local view. An unreachable
seed retains at most its bounded advertisement lease.

The seed authenticates the requester before caching its advertisement, and the
client authenticates the responding seed according to the normal TLS and
allowlist policy. That authentication covers only the two participants in the
bootstrap exchange. Every returned endpoint is still an untrusted suggestion
that must complete its own normal peer handshake before it becomes a
connection.

### MdnsDiscovery

`MdnsDiscovery` browses `_numax._tcp.local.` using a cluster-specific DNS-SD
subtype derived from the BLAKE3 hash of the cluster ID. It also requires an
exact `cluster` TXT property match. This two-part filter prevents accidental
cross-cluster discovery; neither value is authentication evidence.

Resolved instances retain first-observation order. Addresses within an
instance are sorted, deduplicated and limited to `max_candidates` before they
enter retained provider state; the instance count and flattened candidate view
are bounded separately. Port zero, unspecified and multicast addresses, and
IPv6 link-local addresses without a usable scope are ignored. A DNS-SD removal
event removes the complete instance contribution; expiry is delegated to the
mDNS daemon's cache and removal events.

mDNS announcement support is required. Announcements accept a concrete IP
address or a `.local` hostname, never a wildcard host or port zero. The provider
filters its own DNS-SD fullname and advertised endpoint. Re-announcement updates
the same service in place, avoiding a withdrawal gap.
Shutdown sends a goodbye/unregister request, stops browsing, waits within the
bounded daemon grace period, shuts the daemon down, joins the bridge task, and
clears the view. This provider is intended for LAN development and demos, not
untrusted multicast networks.

### DnsSrvDiscovery

`DnsSrvDiscovery` reads a fully qualified SRV name beginning with `_` and
ending with `.`, using the system resolver. It starts with an empty view and
performs refreshes in the background. Results are sorted deterministically by
SRV priority, target, port and weight, then deduplicated and bounded. Root
targets and records with port zero do not become candidates. SRV weight is not
used as a membership assertion or a connection authorization rule.

A successful answer replaces the complete view. Refresh happens no later than
the DNS validity deadline and is capped by `max_refresh_interval`. A successful
empty or no-record answer removes the previous view. A transient lookup error
keeps the last valid view only until its DNS validity deadline, then removes it
while retrying at `retry_interval`. DNS-SRV does not support announcements.
Shutdown cancels an in-flight resolver lookup, then stops and joins the refresh
task.

### FileWatchDiscovery

`FileWatchDiscovery` polls an externally managed UTF-8 file. Each trimmed,
non-empty line is one `host:port` endpoint; a line whose first non-whitespace
character is `#` is a comment. Entries are canonicalized and deduplicated in
first-occurrence order. File size, candidate count, event capacity and polling
interval are bounded and configurable.

A missing file is a valid empty view, both initially and after removal. This
also observes delayed creation and Kubernetes-style atomic file replacement.
The initial read fails for other I/O, encoding, syntax or limit errors. After a
valid snapshot exists, an unreadable, non-UTF-8, malformed, oversized or
over-limit update is rejected atomically and the last valid snapshot remains
active; polling continues. File discovery does not support announcements.
Shutdown stops and joins the polling task.

### Provider dependencies

The two added runtime dependencies have narrow protocol roles. `mdns-sd`
provides DNS-SD browse, cache-expiry, unregister/goodbye and daemon shutdown
behavior that should not be reimplemented as ad-hoc multicast parsing.
`hickory-resolver` provides real SRV records and their DNS validity deadlines;
Tokio's host lookup does not expose either. File discovery uses Tokio polling
instead of adding a filesystem-notification dependency, which also makes
delete/create and atomic replacement semantics consistent across platforms.

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
not proof of membership and not a replacement for TLS identity or authorization.
The bootstrap handshake carries and validates it; the normal replication
`Hello` remains unchanged.

## Security and compatibility boundaries

A discovered endpoint is only a connection candidate. Discovery does not assert
node identity, authenticate a peer, authorize a connection, or establish
membership. Existing TLS and mTLS verification, peer allowlists, connection
limits, and handshake checks remain authoritative when the runtime attempts a
connection.

Static, mDNS, DNS-SRV and file discovery do not change persisted data or the
WebAssembly host and guest APIs. Bootstrap adds a wire exchange and therefore
increments `PROTOCOL_VERSION` to `5`; version `4` peers are rejected before
bootstrap or replication admission. No storage migration or guest ABI change
is involved. See [Wire Versioning](/numax/design/wire-versioning/) for the exact
compatibility boundary.

The CLI resolves provider selection from flags, `NX_DISCOVERY_*` variables and
the `[discovery]` TOML section. Explicit peers continue to contribute a static
source when a dynamic provider is selected; they are never reinterpreted as
bootstrap seeds. Provider construction occurs in `nx-core` after the durable
local `NodeId` has been loaded.

## Verification coverage

Deterministic unit and component tests cover static compatibility, bounded
watch overflow, snapshot revision continuity, late candidate arrival,
overlapping source contributions, source removal, startup rollback and
cancellation-safe shutdown. Provider-specific tests additionally cover:

- bootstrap TTL expiry during a stalled seed query, bounded responses,
  authenticated TLS/allowlist rejection, seed loss, restart and withdrawal;
- DNS-SRV ordering, filtering, refresh, validity expiry, transient failure,
  recovery and cancellation of an in-flight lookup;
- file creation and removal, atomic replacement, malformed and non-UTF-8
  updates, last-good retention, recovery and shutdown;
- mDNS address and instance bounds, self filtering, removal and service-name
  conflicts.

The ignored
`discovery::mdns::tests::two_daemons_discover_and_remove_an_announced_endpoint`
test exercises two real DNS-SD daemons over local multicast, including goodbye
removal. CI runs this check explicitly on a dedicated macOS runner; keeping it ignored prevents the ordinary cross-platform suite from failing on hosts or containers without multicast support. Run it manually on a multicast-capable host with:

```sh
cargo test -p nx-core \
  discovery::mdns::tests::two_daemons_discover_and_remove_an_announced_endpoint \
  -- --ignored --exact
```

The three-node CRDT LAN demo remains the release closing criterion and is not
substituted by this two-daemon provider test.
