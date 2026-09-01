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
| `v0.1.4` | Management API | active |
| `v0.1.5` | Peer Discovery - Foundations | planned |
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

**Goal**: provide a programmatic alternative to the CLI for integration with automation tooling.

**Daemon lifecycle**:
- [x] `nx serve` starts a node without requiring a WASM module
- [x] The daemon remains active when sync is disabled
- [x] The Management API starts with `nx serve` when configured and shuts down gracefully with the daemon

**REST API `/api/v1/*`**:
- [x] Served on a separate port (default `127.0.0.1:9102`)
- [x] Auth with bearer token (never open without)
- [x] Management API disabled when no bearer token is configured
- [x] **Default**: bind only to `127.0.0.1`, external exposure must be explicit
- [ ] OpenAPI 3.1 spec in `docs/api/openapi.yaml`

**API contract and safety**:
- [ ] Contract-first design: OpenAPI is reviewed before endpoint implementation
- [ ] Stable JSON error envelope with documented HTTP status codes
- [ ] Cursor-based pagination with bounded page and response sizes
- [ ] Binary-safe key and value encoding; internal `__nx/` keys are never exposed
- [ ] Bounded request bodies, request timeouts and concurrency limits
- [ ] Documented idempotency and retry semantics for every mutating endpoint
- [ ] Explicit authentication policy for health and readiness endpoints
- [x] Non-loopback exposure requires an explicit opt-in and documented TLS or reverse-proxy protection

**v1 endpoints**:
- [ ] `POST /api/v1/modules` - register a WASM module
- [ ] `GET /api/v1/modules` - list registered modules
- [ ] `GET /api/v1/modules/{id}` - inspect a registered module
- [ ] `DELETE /api/v1/modules/{id}` - remove a registered module
- [ ] `POST /api/v1/modules/{id}/runs` - execute a registered module once
- [ ] `GET /api/v1/peers` - list connected peers
- [ ] `GET /api/v1/keys?prefix=...` - list keys
- [ ] `GET /api/v1/keys/{key}` - read a value
- [ ] `GET /api/v1/health`, `GET /api/v1/ready` (aliases of existing observability endpoints)

**Module lifecycle**:
- [ ] Registered modules are local artifacts with stable IDs
- [ ] Module execution remains one-shot; long-running modules and hot reload are out of scope

**Internal pattern**:
- [ ] `RuntimeIntrospection` is the single source of truth for read-only operations used by CLI, REST API, dashboard and TUI
- [ ] `RuntimeManagement` is the single source of truth for mutating management operations

**Automation example**:
- [ ] Reproducible example managing a node through the REST API with `curl` and a shell script

**Closing criterion**:
> After starting the daemon, a numax node can be managed exclusively via the authenticated REST API. The documented automation example registers, inspects and runs a module, then verifies node readiness without further CLI commands.

---

## v0.1.5 - Peer Discovery: Foundations 🌐

**Goal**: stop requiring `--peer 1.2.3.4:9000` for every node. Introduce the abstraction and mechanical bootstrap (not yet gossip-based; that comes in 0.1.6).

**Abstraction**:
- [ ] `PeerDiscovery` trait with `discover()`, `announce()`, `watch()` methods
- [ ] Internal replacement of `--peer` with a `StaticDiscovery` implementing the trait

**Initial implementations**:
- [ ] `StaticDiscovery` - peer list from config (backward-compatible)
- [ ] `BootstrapGossipDiscovery` - join with 1 address, learn the others through `Hello` exchange
- [ ] `MdnsDiscovery` - LAN discovery for demo and dev
- [ ] `DnsSrvDiscovery` - discovery via DNS-SRV record
- [ ] `FileWatchDiscovery` - peer file updated externally (useful for K8s headless services)

**Configuration**:
- [ ] `[discovery]` section in `numax.toml` with `mode = "static" | "bootstrap" | "mdns" | "dns-srv" | "file"`

**Explicit decision**:
- [ ] Document `nat-traversal.md` - NAT/WAN traversal to be evaluated for `0.2.0`.

**Closing criterion**:
> Three nodes on the same LAN discover each other via mDNS without any `--peer` flag. Reproducible demo in `examples/discovery_lan/`.

---

## v0.1.6 - Peer Discovery: SWIM & Gossip K-fanout 🕸

**Goal**: **dynamic** discovery, with membership, failure detection and dissemination separated. This is **the strength of `0.2.0`**.

**Design doc as a public RFC**:
- [ ] `peer-discovery.md`
- [ ] Documented failure scenarios
- [ ] Detailed test plan

**Three separate channels**:
- [ ] **Membership**: SWIM / Lifeguard (who is in the cluster)
- [ ] **Failure detection**: phi-accrual or SWIM-style suspicion (who is dead/suspect)
- [ ] **Data dissemination**: K-fanout gossip for CRDT ops (what to propagate)

**Adaptive K-fanout gossip**:
- [ ] Configurable fanout (default `K = ceil(log2(N) + c)`)
- [ ] Adaptive rate based on load/RTT
- [ ] Backpressure: controlled drops, never storms
- [ ] Periodic anti-entropy complementing gossip

**Runtime peer management**:
- [ ] `POST /api/v1/peers` - manually add a peer to the live membership set
- [ ] Peers added through the API participate in failure detection, reconnection and anti-entropy
- [ ] Document whether runtime-added peers persist across node restarts

**Determinism for tests**:
- [ ] Seedable gossip PRNG for reproducible tests

**Test scenarios**:
- [ ] 50 nodes, 10% packet loss, partition recovery
- [ ] Cluster split-brain → merge without op loss
- [ ] 100% rolling restart of nodes → cluster survives
- [ ] False positive detection rate measured

**Closing criterion**:
> A 50-node cluster on a simulated network with 10% packet loss converges in < 30s after a 60s partition. No false-positive failure detection in nominal conditions for 1h.

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
