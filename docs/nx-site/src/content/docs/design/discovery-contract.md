---
title: Peer Discovery Contract
description: Snapshot, event delivery, cancellation, and compatibility guarantees for peer discovery providers.
---

## Scope and ownership

This contract describes peer discovery in `v0.1.5`, the current Numax version.

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
revisioned `Observed(DiscoverySnapshot)` events with per-endpoint observation
timestamps. A replacement changes the provider contribution
atomically, including its ordering: consumers never observe a synthetic empty
view between removals and additions. `Replaced`, `Added` and `Removed` remain
available for providers without observation metadata. Providers deduplicate their own snapshots where their
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

Freshness is based on successful endpoint observation, not cache publication or
watch subscription time. `Observed` preserves those timestamps in both events
and resubscription snapshots. A successful refresh of an unchanged endpoint
list advances freshness; replaying a cached last-good view after an error does
not renew its lease. Aggregated bootstrap seed and mDNS instance views preserve
each endpoint's observation time rather than refreshing unrelated entries.

The resulting bounded snapshot drives initial dialing and automatic
reconnection in candidate order. An empty startup snapshot is valid, and the
loops remain alive for later additions. `SyncManager::start()` returns after
local services and their owned background loops are ready; it does not await
peer convergence or successful dialing of every candidate. A stalled initial
handshake therefore does not delay local readiness by one timeout per peer.

Removing a candidate stops new reconnect attempts; it does not terminate an
already active, admitted connection. Once that connection closes it is not
re-established unless a source adds the endpoint again. Anti-entropy instead
uses all active connection send-address keys, including inbound connections
and peers no longer present in discovery. Its periodic cadence is independent
of candidate churn, and missed ticks are skipped rather than replayed in a
burst. Removal from discovery therefore does not disable repair over a live
connection.

Anti-entropy pulls the bounded operation log and relies on receiver
deduplication. It is not state transfer and does not guarantee unrestricted
lossless recovery after a partition or restart: required operations and
deduplication history must still be retained. Rediscovery alone does not prove
that a missing-history gap can be repaired.

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
the configured number of results. Response capacity is in `1..=4096`, matching
`nx_net::MAX_BOOTSTRAP_RESPONSE_CAPACITY`; bootstrap configuration rejects
larger capacities before querying a seed. A successful view contains the seed itself
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
instance are sorted and deduplicated. The application-owned retained endpoint
contributions are bounded **globally** by `max_candidates`, including duplicate
contributions from different instances, not by `max_instances * max_candidates`.
Replacing an instance reclaims its previous allocation before admission; the
instance count and flattened candidate view are also bounded. Port zero, unspecified and multicast addresses, and
IPv6 link-local addresses without a usable scope are ignored. A DNS-SD removal
event removes the complete instance contribution; expiry is delegated to the
mDNS daemon's cache and removal events.

These are Numax application-state bounds, not a whole-library memory cap.
`mdns-sd 0.21` does not expose a configurable bound for its internal DNS record
cache; `max_instances` and `max_candidates` do not bound that cache. Do not
interpret them as protection against arbitrary untrusted multicast traffic.

mDNS announcement support is required. Announcements accept a concrete IP
address or a `.local` hostname, never a wildcard host or port zero. The provider
filters its own DNS-SD fullname and advertised endpoint. Re-announcement updates
the same service in place, avoiding a withdrawal gap.
Shutdown has one cleanup owner: it requests unregister/goodbye, waits for the
daemon acknowledgement within a deadline, stops browsing, requests daemon
shutdown and awaits its acknowledgement, joins the bridge task, and clears the
view. The common budget reserves time for daemon termination even when
unregister fails or its acknowledgement never arrives; queue retries are also
bounded by those deadlines. Cleanup errors are reported, not silently treated
as success. A daemon acknowledgement does **not** guarantee receipt of a UDP
goodbye by every LAN peer. Drop is best-effort fallback, not a stronger delivery
guarantee. This provider is intended for LAN development and demos, not
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

Regression coverage also exercises observation freshness versus cached replay,
resubscription timestamps, global mDNS retained-state bounds, bounded shutdown
acknowledgements, non-blocking startup dialing and anti-entropy over active
connections independently of discovery churn. Test presence is not evidence
that every environment-dependent scenario has run successfully.

The ignored
`discovery::mdns::tests::two_daemons_discover_and_remove_an_announced_endpoint`
test exercises two real DNS-SD daemons over local multicast, including goodbye
removal. CI runs this check explicitly on a dedicated macOS runner; keeping it ignored prevents the ordinary cross-platform suite from failing on hosts or containers without multicast support. Run it manually on a multicast-capable host with:

```sh
cargo test -p nx-core \
  discovery::mdns::tests::two_daemons_discover_and_remove_an_announced_endpoint \
  -- --ignored --exact
```

CI also explicitly selects
`discovery_lan::mdns_three_daemons_recover_missed_crdt_ops_after_restart` from
the CLI multiprocess suite on macOS, with `NUMAX_MDNS_E2E=1` and
`NUMAX_MDNS_LAN_IP` derived from a real local interface. It builds both reader
and writer variants of the `discovery_lan` guest first. The generic Ubuntu
ignored-test invocation excludes this multicast-specific module.

That E2E uses three real daemon **processes on one host**, without `--peer`,
and checks discovery, CRDT replication, missed-operation recovery after restart
within a configured 128-operation retention bound, stable identities and
shutdown. It is not evidence of a run on three separate LAN devices or of
recovery beyond retained history. The three-device LAN demo remains a separate
release closing check; neither provider-test presence nor CI wiring asserts it
has passed.
