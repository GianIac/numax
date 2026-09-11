---
title: Roadmap
description: Current status and planned versions.
---


> **Note on the mutability of this roadmap:**
>
> This roadmap can change, even significantly, based on:
> - community feedback (issues, discussions, real-world usage),
> - technical constraints that emerge during implementation,
> - external dependencies (Wasmtime, sled, the WASM/WASI ecosystem, Component Model standard),
> - new ideas, opportunities, or simply how one wakes up in the morning with a better intuition.
>
> **Proposing changes**: anyone can open a **Pull Request against this roadmap page** to:
> - suggest a new item in a future version,
> - move an item between versions with motivation,
> - flag a risk or dependency that justifies a change in priority,
> - propose a completely new version.
>
> Roadmap PRs are **as welcome as code PRs** !

---

## Status and goal

- **Current release line**: `v0.1.4` (active - Management API)
- **Final goal of the cycle**: stable `v0.2.0`.
- **Philosophy of intermediate releases**: every `0.1.x` is a **stable and usable** release. Capabilities are added incrementally without sacrificing quality.

Unlike `v0.1.0` (declared for non-critical workloads), `v0.2.0` must guarantee:
- **dynamic peer discovery** without manual configuration,
- a **reactive event model**, no longer just `run()` one-shot,
- granular **capability-based security** with per-module quotas,
- **complete operability** (snapshot, restore, replay, diff, hot reload),
- **wire and schema versioning** with documented compatibility,
- **hardened supply chain** (signatures, SBOM, continuous fuzzing),
- **complete observability** (metrics, dashboard, TUI).

---

## Version map

| Version | Theme | Status |
|---|---|---|
| `v0.1.0` | First production-ready + Documentation, Distribution & Configuration | released |
| `v0.1.1` | Architectural Cleanup & Versioning | released |
| `v0.1.2` | Performance & Profiling | released |
| `v0.1.3` | Supply Chain & Fuzzing | released |
| `v0.1.4` | Management API | released |
| `v0.1.5` | Peer Discovery - Foundations | active |
| `v0.1.6` | Peer Discovery - SWIM & Gossip K-fanout | planned |
| `v0.1.7` | Reactive Module Model - Events | planned |
| `v0.1.8` | Reactive Module Model - HTTP & Hot Reload | planned |
| `v0.1.9` | Capability-Based Security | planned |
| `v0.1.10` | Resource Quotas & Multi-tenant | planned |
| `v0.1.11` | Op-log Compaction & Snapshots | planned |
| `v0.1.12` | Operability Tools | planned |
| `v0.1.13` | Built-in Dashboard | planned |
| `v0.1.14` | TUI & Advanced CRDTs | planned |
| `v0.1.15` | WIT & Component Model | planned |
| `v0.2.0-rc.1` | Release Candidate hardening | planned |
| `v0.2.0` | **Stable - production-ready, any criticality** | final goal |

> **Legend**: released = previous stable release; active = current release line; planned = future work; final goal = end of the cycle.

---

## v0.1.1 - Architectural Cleanup & Versioning 🧹

`v0.1.1` paid down the architectural debt inherited from `v0.1.0` and made
version boundaries explicit, so clusters fail safely instead of mixing
incompatible nodes.

The monolithic `sync_manager.rs` was split into a proper `sync_manager/` module
with an `OpApplier` trait per CRDT family, under a strict behavior-preserving
constraint. On top of that cleaner base, the release introduced **wire
protocol versioning** (explicit `protocol_version` in `Hello`, documented
compatibility matrix, typed `WireError` frames with per-error retry semantics)
and **schema persistence versioning** (magic number + version on every sled
table, generic N → N+1 migration registry, and the new `nx migrate` CLI for
offline upgrades from a `v0.1.0` datastore).

Net result: `sync_manager.rs` is no longer one giant file, and a mixed
`0.1.1`/`0.1.0` cluster refuses the connection with a clear versioned error
instead of crashing.

📄 Full details in the [`v0.1.1` release notes](https://github.com/GianIac/numax/releases/tag/v0.1.1).

---

## v0.1.2 - Performance & Profiling 📊

`v0.1.2` made performance observation automatic and silent regressions
impossible to merge.

The runtime now profiles itself: `tokio-console` for live task visibility,
feature-gated CPU flamegraphs (`pprof-rs`) and load-phase heap profiles
(`dhat`) produced as Ubuntu CI artifacts, and per-WASM-module execution
metrics covering duration, outcomes, compilation cache and linear-memory
usage. On top of that, load benchmarks now emit a structured JSON report and
CI compares every PR against a baseline committed under
`crates/*/reports/baselines/`, failing automatically on p99 latency,
throughput or RSS regressions.

Net result: flamegraphs and heap profiles are one click away on every CI run,
each module reports its own cost through Prometheus, and a PR that pushes
sync p99, throughput or RSS past the configured budget is blocked
automatically with the regression details.

📄 Full details in the [`v0.1.2` release notes](https://github.com/GianIac/numax/releases/tag/v0.1.2).

---

## v0.1.3 - Supply Chain & Fuzzing

`v0.1.3` hardened the boundaries where Numax meets untrusted input and made
release artifacts independently verifiable.

The supply chain is now checked continuously through `cargo-deny`, a daily
`cargo-audit`, SHA-pinned GitHub Actions and minimal workflow permissions.
Every release publishes validated, target-specific CycloneDX SBOMs together
with checksums signed through Sigstore Cosign. On the runtime side, four
`cargo-fuzz` targets exercise the production wire decoder and framing reader,
property tests cover the merge laws of LWW-Map, ORSet and RGA, and the binary
wire encoding is protected by bounded allocation limits and golden hashes.

Net result: dependencies and release artifacts can be audited without blind
trust, malformed wire input is exercised automatically, and an invalid
persisted CRDT record or rejected storage write cannot silently leave Numax in
a partially updated state.

📄 Full details in the [`v0.1.3` release notes](https://github.com/GianIac/numax/releases/tag/v0.1.3).

---

## v0.1.4 - Management API 🔌

`v0.1.4` brings Numax back to shipping a major runtime capability: a node can
now be operated programmatically, without ever invoking the CLI.

The release introduces `nx serve`, a daemon that starts a node without a WASM
module and stays alive until a shutdown signal, and an authenticated REST API
(default `127.0.0.1:9102`) served by the new `nx-api` crate. Every endpoint,
including the health and readiness probes, requires a bearer token, binds to
loopback unless an external bind is explicitly opted into, and follows a
contract-first OpenAPI 3.1 spec validated in CI. The v1 surface registers,
inspects, runs and deletes modules and exposes read-only peer, datastore and
probe operations, all with a stable JSON error envelope, cursor pagination,
binary-safe keys, and bounded bodies, timeouts and concurrency. Underneath,
the new `RuntimeIntrospection` and `RuntimeManagement` interfaces in `nx-core`
become the single source of truth for the CLI, the REST API and, later, the
dashboard and TUI, backed by a persistent local module registry and
cancellable one-shot guest execution via Wasmtime epoch interruption.

Net result: after starting the daemon, a Numax node can be managed exclusively
through the authenticated REST API — the documented `curl` automation example
registers, inspects and runs a module, then verifies readiness, without a
single further CLI command.

📄 Full details in the [`v0.1.4` release notes](https://github.com/GianIac/numax/releases/tag/v0.1.4).

---

## v0.1.5 - Peer Discovery: Foundations 🌐

**Goal**: stop requiring `--peer 1.2.3.4:9000` for every node. Introduce discovery providers and bootstrap address exchange; SWIM membership and K-fanout data gossip follow in `0.1.6`.

**Abstraction**:
- [ ] `PeerDiscovery` trait with `discover()`, `announce()`, `watch()` methods
- [ ] Internal replacement of `--peer` with a `StaticDiscovery` implementing the trait
- [ ] Define snapshot/watch consistency, provider errors, announcement support, cancellation and bounded event delivery

**Peer coordination and identity**:
- [ ] Updateable peer candidates shared with reconnection and anti-entropy, including startup with an empty peer list
- [ ] Distinguish discovery candidates, authenticated identities, advertised listening endpoints and active connections
- [ ] Define duplicate and self-peer handling, simultaneous connections, source expiry and removal semantics
- [ ] Bound candidates, concurrent connection attempts and connections; preserve backoff, TLS identity checks and authorization
- [ ] Define cluster isolation and advertised endpoint validation, including wildcard binds and dynamically assigned ports
- [ ] Own and stop all discovery tasks; roll back partial startup and withdraw announcements on shutdown

**Initial implementations**:
- [ ] `StaticDiscovery` - peer list from config (backward-compatible)
- [ ] `BootstrapGossipDiscovery` - contact a seed and learn bounded lists of advertised endpoints through the handshake/bootstrap exchange; suggestions remain candidates to authenticate, not membership assertions
- [ ] `MdnsDiscovery` - LAN discovery for demo and dev
- [ ] `DnsSrvDiscovery` - discovery via DNS-SRV record
- [ ] `FileWatchDiscovery` - peer file updated externally (useful for K8s headless services)

**Configuration**:
- [ ] `[discovery]` section in `numax.toml` with `mode = "static" | "bootstrap" | "mdns" | "dns-srv" | "file"`
- [ ] Define provider-specific settings and interaction with explicit peers; preserve CLI > `NX_*` > TOML > defaults and effective-config output

**Protocol compatibility**:
- [ ] Specify bootstrap messages and endpoint advertisement; increment the wire version for incompatible changes
- [ ] Verify JSON and Bincode encoding, handshake limits and safe rejection against `v0.1.4`; static configuration compatibility does not imply mixed-version wire compatibility

**Explicit decision**:
- [ ] Document `nat-traversal.md` - NAT/WAN traversal to be evaluated for `0.2.0`.

**Acceptance tests**:
- [ ] Deterministic provider tests for late arrivals, overlapping sources, removals, transient errors, event overflow and shutdown
- [ ] Static configuration regression coverage; bootstrap recovery after seed loss; DNS refresh/expiry; file replacement and malformed updates
- [ ] Real LAN mDNS checks, TLS rejection and reconnection after restart; justify and validate additional provider dependencies

**Closing criterion**:
> All five providers pass their acceptance tests. Three nodes on the same LAN discover each other via mDNS without any `--peer` flag, replicate a CRDT update and recover after reconnection within the declared retention window. Reproducible demo in `examples/discovery_lan/`.

---

## v0.1.6 - Peer Discovery: SWIM & Gossip K-fanout 🕸

**Goal**: build dynamic membership, failure detection and K-fanout dissemination on the discovery foundations, with explicit recovery guarantees and bounded resource use.

**Design doc as a public RFC**:
- [ ] `peer-discovery.md`, documenting accepted contracts and recording unresolved alternatives
- [ ] Documented failure scenarios
- [ ] Detailed test plan

**Replication correctness prerequisites**:
- [ ] Define and test atomic local persistence of CRDT state, operation identity and replay metadata before acknowledging a local write
- [ ] Distinguish batch acceptance, flush-confirmed durability and remote replication; specify any acknowledgement semantics
- [ ] Decide how to deduplicate delayed replay safely and identify missing operations; evaluate per-origin sequencing versus idempotent state/delta replication without treating an observed maximum OpId as a causal frontier
- [ ] Specify and test wire/schema evolution and historical-data handling for the chosen approach; preserve historical fixtures
- [ ] Test JSON/Bincode protocol changes and safe rejection against the `v0.1.5` binary
- [ ] Define the recoverable retention window and detect unrecoverable gaps, including a new node joining after required history has expired
- [ ] Decide whether bounded-window recovery is sufficient or a versioned CRDT state-transfer subset must move forward from `0.1.11`; do not promise unrestricted lossless recovery before this decision

**Separate control and data responsibilities**:
- [ ] **Membership**: SWIM / Lifeguard (who is in the cluster)
- [ ] **Failure detection**: select and specify the suspicion model; evaluate SWIM-style suspicion with Lifeguard before adding a separate phi-accrual detector
- [ ] **Data dissemination**: K-fanout gossip for CRDT ops (what to propagate)
- [ ] Define identity, restart generations, incarnation ordering, refutation, leave/rejoin and stale-message handling
- [ ] Choose and validate control transport and authentication; isolate probe/control budgets from data backpressure
- [ ] Measure local scheduling delays and detector behavior under CPU-bound WASM execution and slow storage

**Adaptive K-fanout gossip**:
- [ ] Configurable fanout (target default `K = ceil(log2(N) + c)`); define local membership estimate `N`, calibrate `c` and clamp to eligible peers and configured limits
- [ ] Forward newly accepted remote operations after persistence, preserving origin and preventing duplicate forwarding loops
- [ ] Maintain a bounded, rotating neighbor set and recovery contacts so sparse topologies and healed partitions can reconnect
- [ ] Adaptive rate based on load/RTT
- [ ] Backpressure with bounded queues, byte budgets, jitter and stable adaptation; dropping a send attempt must not discard the only recoverable copy of an accepted operation
- [ ] Periodic, byte-bounded and paginated anti-entropy complementing gossip

**Runtime peer management**:
- [ ] `POST /api/v1/peers` through `RuntimeManagement` - accept a manual candidate for normal validation and admission, without implying that connection or authentication has succeeded
- [ ] Once admitted, peers added through the API participate in failure detection, reconnection and anti-entropy
- [ ] Document whether runtime-added peers persist across node restarts
- [ ] Distinguish candidates, membership, active connections and replication recovery in introspection; expose bounded metrics for probes, queues, retries, fanout and recovery gaps

**Determinism for tests**:
- [ ] Shared production/simulation state machine with injected clock, transport and seedable PRNG; deterministic event and candidate ordering

**Test scenarios**:
- [ ] 50 nodes, 10% loss and partition recovery, with versioned seeds, topology, load, payload sizes, latency, retention and loss model
- [ ] Separate simulated message loss from packet loss over the real transport
- [ ] Cluster split-brain → merge without accepted-op loss within the declared recovery contract; exceed retention separately and verify explicit recovery failure rather than false convergence
- [ ] Sparse-topology convergence for every CRDT family, delayed duplicates, storage failures and interrupted persistence
- [ ] 100% rolling restart, one node at a time with recovery between restarts; test simultaneous full shutdown separately
- [ ] False-positive suspicion and failure-declaration rates measured alongside true-failure detection latency and resource use

**Closing criterion**:
> A reproducible 50-node test with a declared 10% loss model converges in < 30s after a 60s partition into two groups of 25, with no loss of accepted operations within the specified recovery contract. The primary recovery test stops new writes at rejoin; a separate test continues writing under recovery load. Validate the real transport under packet loss as well. A defined nominal 1h run records no false failure declarations and reports refuted suspicions separately. Publish workloads, seeds and results; this observation is not a universal zero-failure guarantee.

---

## v0.1.7 - Reactive Module Model: Events ⚡

**Goal**: modules become **long-running and reactive**.

**Design doc**:
- [ ] `docs/design/event-model.md` as RFC

**Module lifecycle**:
- [ ] Long-running module with event loop
- [ ] `init()` called at startup
- [ ] `shutdown()` called on graceful shutdown
- [ ] Backward-compatible `run()` one-shot mode (so existing examples don't break)

**Registerable callbacks**:
- [ ] `on_remote_op(key, op_kind)` - CRDT op applied by a peer
- [ ] `on_tick(ms)` - periodic timer
- [ ] `on_peer_connected(node_id)` / `on_peer_disconnected(node_id)`
- [ ] `on_message(topic, payload)` - explicit intra-cluster messages

**Guest SDK**:
- [ ] Macro `nx_sdk::on_remote_op!` for ergonomic registration
- [ ] Example `examples/reactive_dashboard/` - module that updates in real time

**Closing criterion**:
> A reactive module receives an op from a peer, runs custom logic (e.g. sends a notification), and the example is documented step-by-step.

---

## v0.1.8 - Reactive Module Model: HTTP & Hot Reload 🔁

**Goal**: modules can **serve HTTP** and be **reloaded without dropping peer connections**.

**HTTP handler**:
- [ ] `on_request(req) -> response` as a callback
- [ ] Explicit `network.serve` capability (deny-by-default)
- [ ] Minimal internal routing (path → handler)

**Hot reload**:
- [ ] `nx reload <module>` - replaces the module without closing peer connections
- [ ] CRDT state preserved during reload
- [ ] Test: reload under load, zero ops lost

**Killer demo**:
- [ ] `examples/collaborative_todo/` - local-first multi-device todo list, web UI served by the module, real-time CRDT sync. **Filmable for the launch.**

**Closing criterion**:
> The "collaborative todo list" demo runs on 3 devices, the user edits a todo, the other devices see it in < 500ms. Hot reload in production with no state loss.

---

## v0.1.9 - Capability-Based Security 🔒

**Goal**: the current "enabled/disabled" model is replaced by granular per-module capabilities.

**Per-module policy file**:
- [ ] `module.policy.toml` next to the `.wasm`
- [ ] Sections: `[capabilities]`, `[quotas]`
- [ ] Granular capabilities for keyspace, CRDT family, network, time, random

**Example**:
```toml
[capabilities]
db.read = ["inventory:*", "settings:*"]
db.write = ["inventory:*"]
crdt.gcounter = ["visits:*"]
crdt.rga = []
net.peers = false
network.serve = true
random = true
time = true
```

- [ ] Policy **signed** with the same key as the cert (anti-tampering)
- [ ] **Deny-by-default**: capability not listed = denied
- [ ] Enforcement at the host-call level
- [ ] Audit log of host calls (optional, opt-in)

**CLI/API**:
- [ ] `nx policy validate <policy.toml>`
- [ ] `nx policy diff <old> <new>`

**Closing criterion**:
> A module without a policy does not start. A module with a minimal policy cannot access keys outside its namespace. Dedicated security tests.

---

## v0.1.10 - Resource Quotas & Multi-tenant 📦

**Goal**: numax becomes **multi-tenant safe**: multiple modules on the same node, isolated, with resource quotas.

> **Schema**: if persisted keys change, bump the schema and add a migration fixture.

**Resource quotas**:
- [ ] `cpu_ms_per_run` - max CPU time per invocation
- [ ] `memory_max_mb` - max module memory
- [ ] `ops_per_sec` - CRDT op rate limit
- [ ] `bytes_written_per_sec` - sled write rate limit
- [ ] Enforcement with module interruption + log + metric
- [ ] Quota usage metrics in Prometheus

**Multi-module per node**:
- [ ] Internal module supervisor
- [ ] `nx run <mod1> <mod2> ...` or config file with module list
- [ ] Op routing based on key prefix per module
- [ ] Keyspace isolation (ties in with capabilities)
- [ ] A module crash does not bring the node down

**Closing criterion**:
> 10 modules on the same node, each with different quotas, none can affect the others. "Malicious module" test attempting to exhaust resources → contained correctly.

---

## v0.1.11 - Op-log Compaction & Snapshots 🗜

**Goal**: the op-log does not grow indefinitely. Backup and restore exist.

> **Schema**: if compaction changes persisted data, bump the schema and add a migration fixture.

**Op-log compaction**:
- [ ] Periodic CRDT state snapshot
- [ ] Op-log truncation up to the snapshot point
- [ ] Persisted dedup-set consistent with truncation
- [ ] Differentiated approach per CRDT family (some support causal compaction, others require full snapshot)
- [ ] `docs/design/compaction.md`

**Snapshot/Restore**:
- [ ] `nx snapshot create` - atomic datastore snapshot
- [ ] `nx snapshot list`
- [ ] `nx snapshot restore <id>`
- [ ] Exportable snapshot (single file, portable across nodes)
- [ ] Test: new node joining using a peer's snapshot

**Storage**:
- [ ] `KvBackend` abstraction to decouple from sled (preparation for a possible switch to redb/fjall)

**Closing criterion**:
> A cluster operating for 7 days with active compaction keeps the op-log within a configured budget. Restore from snapshot in < 60s for a 10GB datastore.

---

## v0.1.12 - Operability Tools 🛠

**Goal**: when something goes wrong, you need the tools to figure it out.

**Replay & diff**:
- [ ] `nx replay <op-log> <new-datastore>` - applies an exported op-log to an empty datastore
- [ ] `nx diff <datastore-a> <datastore-b>` - compares two datastores and shows divergences
- [ ] `nx inspect <key>` - structured CRDT dump for a key

**Optional determinism**:
- [ ] `--deterministic` mode that disables uncontrolled random/time
- [ ] Replay perfectly reproducible in deterministic mode
- [ ] Document `docs/design/determinism.md`

**Opt-in telemetry**:
- [ ] **Active** opt-in, default **off**
- [ ] Collected data: version, OS, arch, average peer count, CRDT families used
- [ ] Explicit document on what is collected and why
- [ ] Self-hosted collection endpoint

**Closing criterion**:
> A real divergence case (even simulated) is diagnosed in < 15 minutes using only the official tools.

---

## v0.1.13 - Built-in Dashboard 🎨

**Goal**: a native, lightweight web dashboard, focused on the 6 views that matter.

**Stack**:
- [ ] Server-side rendering + HTMX + SSE (no React/Vue/heavy bundles)
- [ ] Theme using a free design system (Pico.css or Tailwind+DaisyUI)
- [ ] **Compile-time feature flag** `--features dashboard` (base binary stays small)

**The 6 views**:
- [ ] **Cluster view**: nodes, status (alive/suspect/dead), RTT latency, topology
- [ ] **CRDT explorer**: list of keys, CRDT family, current value, last modification, author
- [ ] **Op flow**: live stream of incoming/outgoing ops (filterable by key/peer/family)
- [ ] **Convergence health**: per-node vector clock, highlights lag and suspected divergences
- [ ] **Throughput/latency**: ops/sec, p50/p95/p99, error rate
- [ ] **Module info**: active modules, host call counts, consumed quotas

**Security**:
- [ ] Served on a separate port (default `127.0.0.1:9101`)
- [ ] Default bind to `127.0.0.1`
- [ ] Basic auth + token (never open without)
- [ ] Read-only by default; mutations require an elevated token

**Reuse**:
- [ ] The dashboard is a consumer of the **same** `RuntimeIntrospection` as the Management API

**Closing criterion**:
> The "convergence health view" diagnoses a simulated divergence in 1 click. Screenshots ready for the public launch.

---

## v0.1.14 - TUI & Advanced CRDTs 🖥

**Goal**: those who live in SSH have their version. Those who need collaborative text editing have it too.

> **Schema**: give new CRDT tables independent versions and migration fixtures.

**TUI `nx top`**:
- [ ] Implemented with `ratatui`
- [ ] Reuses the Management API `/api/v1` (same 6 views as the dashboard)
- [ ] Local connection (default) or remote with token
- [ ] Hotkeys k9s/lazygit-style

**Advanced CRDTs (integration, not reimplementation)**:
- [ ] Evaluation and integration of **Yrs** (Rust port of Yjs) as an optional backend for efficient text sequences
- [ ] Evaluation of **Automerge** for nested JSON CRDT
- [ ] Document `docs/design/advanced-crdts.md` with tradeoffs
- [ ] Example `examples/collaborative_editor/` - replicated text editor

**User-defined CRDT** (kick-off, not completion):
- [ ] Document `docs/design/user-defined-crdts.md` with interface proposal
- [ ] Prototype behind an experimental feature flag
- [ ] Required mathematical guarantees documented (commutativity, associativity, idempotency)

**Closing criterion**:
> `nx top` is usable for production debugging via SSH. Working collaborative editor demo with Yrs.

---

## v0.1.15 - WIT & Component Model 🧩

**Goal**: the host API ABI becomes **standard, stable, multi-language** via the WebAssembly Component Model.

**Gradualist approach**:
- [ ] **Step 1**: write the `.wit` describing the current Host API (specification only, no migration)
- [ ] **Step 2**: automatically generate the guest SDK bindings from `.wit` with `wit-bindgen`
- [ ] **Step 3**: port the runtime to `wasmtime::component::Linker` behind feature flag `--features components`
- [ ] **Step 4**: legacy ABI maintained in parallel, deprecated in `0.3.0`

**WASI Preview 2**:
- [ ] Optional evaluation and integration (capability-based filesystem/clock/random/sockets)
- [ ] Naturally ties in with the capability-based security from `0.1.9`

**Multi-language**:
- [ ] Guest example in **Go** (TinyGo)
- [ ] Guest example in **JavaScript** (ComponentizeJS)
- [ ] Guest example in **Python** (componentize-py)

**Closing criterion**:
> The same `.wit` is used by the Rust SDK, by a Go guest, by a JS guest, and they all converge on the same shared CRDT.

---

## v0.2.0-rc.1 - Release Candidate Hardening

**Goal**: everything built in `0.1.0`–`0.1.15` is put under stress, integrated, documented and finished.

**Integrated hardening**:
- [ ] Combined stress test: discovery + capability + quotas + compaction + reload under load
- [ ] Extended chaos test: unstable network, restart loop, malicious module, partition recovery
- [ ] 7-day soak test on a real cluster (not simulated)
- [ ] Internal security audit completed

**Final documentation**:
- [ ] Migration guide `0.1.x → 0.2.0`
- [ ] Updated production deployment guide
- [ ] All design docs revised and linked from the docs site

**RC criteria**:
- [ ] `cargo test --workspace` passes
- [ ] `cargo clippy --workspace --all-targets -- -D warnings` passes
- [ ] 24h fuzzing green on all targets
- [ ] Regression gate green
- [ ] All tutorials verified end-to-end

---

## v0.2.0 - Stable

**Final goal**: distributed runtime **production-ready for any criticality**.

**Public runtime API evolution**:
- [ ] Introduce opt-in guest allocator instrumentation for exact allocated/freed bytes together with a `#[non_exhaustive]` `HostState`, a supported constructor/builder, and `0.1.x` migration guidance

**Final release criteria**:
- [ ] All `0.1.0`–`0.1.15` releases closed
- [ ] Complete and reviewed documentation
- [ ] `0.3.x` roadmap opened as RFC

---

## Beyond `v0.2.0` - candidate directions for `0.3.x`

> Nothing promised. These are **candidate themes** that may enter `0.3.x` or later, based on feedback and priorities.

- **NAT traversal and WAN gossip** (STUN, relay, possibly libp2p)
- **OSS-Fuzz integration**, once Numax has enough adoption to meet upstream eligibility requirements
- **User-defined CRDTs** complete and production-ready
- **Legacy ABI deprecated**: Component Model only
- **Federated clusters**: clusters of clusters, with cross-cluster replication policies
- **Pluggable storage backends**: redb, fjall, custom
- **GPU/ML guests**: WASI-NN integration
- **Edge orchestration**: optional integration with existing edge runtimes
- **Infrastructure as Code**: evaluate a Terraform provider or Ansible integration.
- **Cross-platform profiling CI**: extend the canonical Ubuntu/Linux CPU profile to scheduled macOS and Windows artifacts, keeping results separated by OS.
- **Tiny embedded runtimes**: evaluate interpreter-based WASM engines such as `wasmi` or WAMR for Cortex-M / RISC-V devices with RAM measured in kilobytes. Wasmtime is the right native engine for the current runtime, but it is not a microcontroller-class target.

---

## How to contribute to the roadmap

1. **Open an issue** with the `roadmap-proposal` label if you want to discuss before writing code or a document.
2. **Open a PR against this roadmap page** if you want to propose directly:
   - a new item in a future version,
   - moving an item between versions,
   - a new intermediate version,
   - a change to a closing criterion.
3. **Roadmap PRs are treated as code PRs**: review, discussion, merge.

---

*Last revision*: `2026-07-22`
