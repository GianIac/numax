---
title: Peer Discovery Contract
description: Snapshot, event delivery, cancellation, and compatibility guarantees for peer discovery providers.
---

## Scope and ownership

This contract describes the discovery-provider API and coordinator used by the runtime. Provider snapshots supply endpoints; admission,
membership and data recovery remain separate responsibilities.

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

Anti-entropy repairs missed operations using retained sequenced history or a
bounded CRDT baseline. Rediscovery alone does not prove recoverability; see the
[recovery contract](../peer-discovery/#anti-entropy-and-recovery).

## Manual candidates

`RuntimeManagement::add_peer` creates a persistent `manual` contribution on the
same coordinator. The source ID is reserved; external providers cannot use it.
Manual candidates have no TTL or provider watch.

Duplicate detection, capacity validation, persistence and publication are
serialized with provider commands. Storage and a covering flush precede
publication. Repeating a manual address preserves reconnect backoff; withdrawing
another source cannot erase its manual contribution.

Registration is available after startup and closes at shutdown. Owned work can
outlive a cancelled HTTP waiter, leaving a confirmed or uncertain durable record
for the next startup. Normal handshake, identity and capacity checks apply before
admission. See [Management API](../../guides/management-api/) for requests,
errors and restart behavior, and [Schema Versioning](../schema-versioning/#manual-candidate-metadata)
for storage format.

## Bounded event delivery

The public Rust constant `MAX_DISCOVERY_EVENT_CAPACITY` is 4096; the default
`DEFAULT_DISCOVERY_EVENT_CAPACITY` remains 128. Both are exported from
`nx_core::discovery` and the `nx_core` crate root. All four dynamic provider
constructors validate `event_capacity` in `1..=4096` before channel/state
allocation, spawning or provider I/O, returning
`DiscoveryError::InvalidConfiguration` outside that range. The bound is on
event slots, not on total process memory or the aggregate candidate snapshot.
Tokio broadcast channels may round the requested capacity up to a power of
two; the common maximum still bounds that rounded capacity. mDNS also uses
the validated capacity for its bounded announcement-request channel.

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

## Announcements and lifecycle

Providers declare announcement support as unsupported, optional or required.
A required announcement without a dialable endpoint fails startup.
`StaticDiscovery::announce()` returns an unsupported-operation error.

| Operation or event | Behavior |
| --- | --- |
| Explicit `request_shutdown()` / `shutdown()` | Stop permanently, withdraw announcements and await owned cleanup. Subsequent watches fail. |
| Startup `request_rollback()` / `rollback()` | Retire the failed generation and its announcements; integrated dynamic providers can restart after successful cleanup. |
| Unexpected worker exit | Invalidate watches, clear the stale view, join the worker and complete cleanup before any replacement. |
| Cleanup failure or panic | Remain unavailable; no replacement generation may overlap unreleased resources. |
| Cancelled shutdown waiter | Cleanup remains owned by the supervisor; another waiter observes the same completion. |

During generation finalization, new `discover()` and `watch()` calls fail with
a retryable error. After cleanup, a new generation is allowed for a retryable
worker failure or panic; a fatal worker failure blocks restart. The original
failure remains observable during shutdown. Concurrent subscribers share one
generation rather than starting overlapping workers.

Bootstrap and mDNS retain announcement intent across recoverable unexpected
exits. Explicit shutdown and startup rollback clear it. Cleanup confirms only
the provider's local retirement contract, not receipt of a withdrawal by every
remote peer.

The coordinator bounds candidate/watch completion and provider cleanup waits.
Third-party providers inherit conservative rollback defaults that call shutdown;
they must implement retry-safe cleanup to support startup retries. Dropping a
provider signals cancellation while its supervisor owns cleanup. Runtime teardown
does not guarantee external withdrawal.

## Provider contracts

All provider limits are checked before a view is exposed to the coordinator.
The runtime-wide candidate limit remains an additional bound after different
sources are combined.

### StaticDiscovery

`StaticDiscovery::new(peers)` is the compatibility adapter for configured
peers. It performs no I/O, never refreshes or expires entries, preserves the
input list byte-for-byte, and does not support announcements. An empty list is
valid.

`StaticDiscovery::with_event_capacity(peers, capacity)` remains infallible and
clamps capacity to `[1, 4096]`: zero becomes one, and values above the maximum
(including `usize::MAX`) become 4096. It does not truncate or reorder peers or
remove duplicates. `StaticDiscovery::try_with_event_capacity(peers, capacity)`
is the strict alternative: it returns `Result<Self, DiscoveryError>` and rejects
zero or values above `MAX_DISCOVERY_EVENT_CAPACITY` with
`InvalidConfiguration` before channel allocation. Valid inputs preserve the
same peer snapshot semantics. Static discovery has no dynamic worker lifecycle.

### BootstrapGossipDiscovery

`BootstrapGossipDiscovery` contacts a bounded, ordered seed list through the
one-shot `BootstrapHello`/`BootstrapAck` exchange. Startup with no responses is
valid: the initial provider snapshot is empty and probing continues in the
background. Seed addresses are canonicalized and deduplicated while retaining
their first configured occurrence.

Each request optionally advertises the caller's endpoint and asks for at most
the configured number of results. Response capacity is in `1..=4096`, matching
`nx_net::MAX_BOOTSTRAP_RESPONSE_CAPACITY`; bootstrap configuration rejects
larger capacities before querying a seed. This is a bootstrap response bound,
not a universal cap on the aggregate runtime snapshot from all discovery sources.
The bootstrap provider's `max_candidates` must also fit the client's response
capacity. Each retained seed view includes the seed itself first, followed by
deduplicated suggestions, truncated to that provider limit. Views from multiple
seeds are flattened in configured seed order, deduplicated again and capped by
the provider's `max_candidates`. The coordinator applies its separate global
candidate limit after combining sources. Returned
entries expire at the earlier of the seed-provided lease and the provider's
`stale_after` bound. Failed probes retain an unexpired last valid view; expired
views are removed at their deadline even while another seed query is still in
flight. Each successful seed response is published without waiting for the
remaining seeds in the refresh pass.

Probe failures use exponential retry bounded by `retry_initial` and
`retry_max`; a success restores `refresh_interval`. Fatal wire failures such as
protocol mismatch or bootstrap request rejection disable that seed for the
current worker generation. Bootstrap announcement support is required. A seed
is tracked conservatively for withdrawal before an advertising query is
awaited: the seed may have accepted the endpoint even if the response is lost,
decoding fails, or the query is cancelled. Tracking is therefore not restricted
to acknowledged successful announcements and is bounded by the configured
seed list.

Cleanup stops and joins the probe loop, then attempts withdrawal from tracked
seeds within a shared four-second budget, dividing the remaining time among
remaining seeds. This is **bounded best effort**, not guaranteed delivery to
every seed: query failures and timeouts are logged, the budget may expire, and
an `Ok(())` cleanup result does not prove remote withdrawal. Local tracking and
the candidate view are cleared even after a worker panic or a cancelled
shutdown waiter. A seed that misses withdrawal can retain the advertisement
until its bounded lease expires. This best-effort bootstrap contract is distinct
from mDNS's checked daemon-acknowledgement cleanup below.

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
filters its own DNS-SD names and advertised endpoints. Original registration
keys remain distinct from per-interface aliases reported by DNS-SD name-conflict
events: unregister uses the original key, not the renamed wire alias.
Re-announcement registers a replacement under a distinct original key before
withdrawing the previous registration and awaiting its acknowledgement.
A rejected registration leaves the previous one owned; failed withdrawal stops
the browse loop and starts checked cleanup rather than accumulating more
registrations. At most two original registrations are owned during replacement.
Own-name history (including aliases) and endpoint history each retain at most
1024 entries until daemon termination, so late cached resolutions are still
self-filtered. History exhaustion rejects an announcement or terminates browsing
on a new alias that cannot be retained; it does not silently evict self-filtering
history. Once queued, the browse task owns announcement completion even if the
calling future is cancelled.

Shutdown has one cleanup owner and one absolute four-second deadline measured
from the first shutdown request. A replacement withdrawal in progress selects
on that request; cancellation retains both original keys for cleanup instead of
losing ownership. Cleanup requests unregister/goodbye for every owned key, stops
browsing, requests daemon shutdown, awaits its acknowledgement, joins the bridge
task, and clears the view. Repeated shutdown calls cannot renew the deadline.
The common budget reserves time for daemon termination even when unregister
fails or its acknowledgement never arrives; queue retries are also bounded by
that same deadline. The coordinator's five-second provider timeout therefore
exceeds the complete provider-owned sequence. Cleanup errors are reported, not
silently treated as success. A daemon acknowledgement does **not** guarantee receipt of a UDP
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

## Related

- [Peer Discovery](../peer-discovery/) — membership, dissemination and recovery contracts.
- [Configuration](../../reference/config/) — provider selection, bounds and precedence.
- [Management API](../../guides/management-api/) — persistent candidates and observation views.
